#include <gtest/gtest.h>
#include "irods/catalog/catalog_facade.hpp"
#include "irods/catalog/gq2_compiler.hpp"
#include "irods/rodsGenQuery.h"
#include "L3KVG/Engine.hpp"
#include "engine/store.hpp"

using namespace irods::catalog;

TEST(MultiHopQueryTest, CollectionToResource) {
    CatalogFacade catalog;
    Config cfg;
    cfg.db_path = "multihop.l3kvg";
    cfg.node_id = 1;
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

    // Register a Resource (Manually put node since register_resource is missing in prototype)
    lite3cpp::Buffer resc_buf;
    resc_buf.init_object();
    resc_buf.set_i64(0, "id", 301);
    resc_buf.set_str(0, "name", "demoResc");
    catalog.get_engine()->put_node("301", resc_buf.move_to_string());
    
    // Add Secondary Index for Resource name (Manually)
    lite3cpp::Buffer idx_buf;
    idx_buf.init_object();
    idx_buf.set_i64(0, "id", 301);
    catalog.get_engine()->put_node("idx:Resource:name:demoResc", idx_buf.move_to_string());

    // Register Replica on that Resource
    replica repl;
    repl.data_id = 1001;
    repl.resource_id = 301;
    repl.physical_path = "/vault/report.pdf";
    repl.replica_number = 0;
    repl.status = "good";
    ASSERT_TRUE(catalog.register_replica(repl).ok());

    // Synchronize
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
