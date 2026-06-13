#include "plugin_test_fixture.hpp"
#include "irods/catalog/catalog_facade.hpp"
#include "irods/irods_server_properties.hpp"
#include "irods/rodsGenQuery.h"
#include <cstring>

namespace irods::catalog::bridge {
    irods::experimental::genquery2::select synthesize_gq2_ast(genQueryInp_t* _inp, CatalogFacade* _catalog, std::vector<uint64_t>& _starting_nodes);
}

using namespace irods::catalog;
using namespace irods::catalog::test;

class GenQueryCompatibilityTest : public PluginTestFixture {};

TEST_F(GenQueryCompatibilityTest, ParentCollectionQueryReturnsResults) {
    // 1. Setup Config
    nlohmann::json config;
    config["zone_name"] = "tempZone";
    config["zone_user"] = "rods";
    config["plugin_configuration"]["database"]["l3kvg"]["plugin_specific_configuration"] = {
        {"db_path", ":memory:"},
        {"node_id", 1},
        {"zmq_endpoint", endpoint()}
    };
    irods::server_properties::instance().set_configuration(config);

    CatalogFacade catalog;
    Config cfg;
    cfg.node_id = 1;
    cfg.zmq_endpoint = endpoint();
    ASSERT_TRUE(catalog.init(cfg, "tempZone").ok());

    // 2. Register Parent Collection
    coll_id_t parent_id;
    collection parent;
    parent.id = 100;
    parent.name = "/tempZone/home/rods";
    parent.owner_name = "rods";
    parent.owner_zone = "tempZone";
    ASSERT_TRUE(catalog.register_collection(parent, parent_id).ok());

    // 3. Register Child Collection with parent_name set
    coll_id_t child_id;
    collection child;
    child.id = 101;
    child.parent_id = 100;
    child.name = "/tempZone/home/rods/sub";
    child.parent_name = "/tempZone/home/rods";
    child.owner_name = "rods";
    child.owner_zone = "tempZone";
    ASSERT_TRUE(catalog.register_collection(child, child_id).ok());

    // 4. Simulate GenQuery: SELECT COLL_NAME WHERE COLL_PARENT_NAME = '/tempZone/home/rods'
    genQueryInp_t inp{};
    memset(&inp, 0, sizeof(genQueryInp_t));

    inp.selectInp.len = 1;
    inp.selectInp.inx = (int*)malloc(sizeof(int));
    inp.selectInp.inx[0] = COL_COLL_NAME;
    inp.selectInp.value = (int*)malloc(sizeof(int));
    inp.selectInp.value[0] = 1;

    inp.sqlCondInp.len = 1;
    inp.sqlCondInp.inx = (int*)malloc(sizeof(int));
    inp.sqlCondInp.inx[0] = COL_COLL_PARENT_NAME;
    inp.sqlCondInp.value = (char**)malloc(sizeof(char*));
    inp.sqlCondInp.value[0] = strdup("= '/tempZone/home/rods'");

    std::vector<snowflake_id_t> starting_nodes;
    auto ast = bridge::synthesize_gq2_ast(&inp, &catalog, starting_nodes);

    ResultSet results;
    ASSERT_TRUE(catalog.execute_query(ast, results).ok());

    EXPECT_GT(results.row_count(), 0) << "Expected to find child collection by parent name filter";
    if (results.row_count() > 0) {
        EXPECT_EQ(results.get_field(0, 0), "/tempZone/home/rods/sub");
    }

    // Cleanup
    free(inp.selectInp.inx);
    free(inp.selectInp.value);
    free(inp.sqlCondInp.inx);
    free(inp.sqlCondInp.value[0]);
    free(inp.sqlCondInp.value);
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
