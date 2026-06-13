#include "plugin_test_fixture.hpp"
#include "irods/catalog/catalog_facade.hpp"
#include "irods/irods_server_properties.hpp"
#include "irods/rodsGenQuery.h"
#include <cstring>
#include <iostream>

namespace irods::catalog::bridge {
    irods::experimental::genquery2::select synthesize_gq2_ast(genQueryInp_t* _inp, CatalogFacade* _catalog, std::vector<uint64_t>& _starting_nodes);
}

using namespace irods::catalog;
using namespace irods::catalog::test;

class DataAccessQueryTest : public PluginTestFixture {};

TEST_F(DataAccessQueryTest, QueryDataAccessReturnsResults) {
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

    // Bootstrap (creates user 'rods')
    ASSERT_TRUE(catalog.bootstrap_catalog("tempZone", "rods").ok());

    // Register Collection
    coll_id_t coll_id;
    collection coll;
    coll.id = 1000;
    coll.name = "/tempZone/home/rods";
    coll.owner_name = "rods";
    coll.owner_zone = "tempZone";
    ASSERT_TRUE(catalog.register_collection(coll, coll_id).ok());

    // Register Data Object
    data_id_t data_id;
    data_object obj;
    obj.id = 2000;
    obj.coll_id = coll_id;
    obj.name = "testfile.txt";
    obj.owner_name = "rods";
    obj.owner_zone = "tempZone";
    ASSERT_TRUE(catalog.register_data_object(obj, data_id).ok());

    // Simulate GenQuery from icp
    // SELECT DATA_ID, USER_NAME, USER_TYPE WHERE COLL_NAME = '/tempZone/home/rods' AND DATA_TOKEN_NAMESPACE = 'access_type'
    genQueryInp_t inp{};
    memset(&inp, 0, sizeof(genQueryInp_t));

    inp.selectInp.len = 3;
    inp.selectInp.inx = (int*)malloc(3 * sizeof(int));
    inp.selectInp.inx[0] = COL_D_DATA_ID;
    inp.selectInp.inx[1] = COL_USER_NAME;
    inp.selectInp.inx[2] = COL_USER_TYPE;
    inp.selectInp.value = (int*)malloc(3 * sizeof(int));
    inp.selectInp.value[0] = 1;
    inp.selectInp.value[1] = 1;
    inp.selectInp.value[2] = 1;

    inp.sqlCondInp.len = 2;
    inp.sqlCondInp.inx = (int*)malloc(2 * sizeof(int));
    inp.sqlCondInp.inx[0] = COL_COLL_NAME;
    inp.sqlCondInp.inx[1] = COL_DATA_TOKEN_NAMESPACE;
    inp.sqlCondInp.value = (char**)malloc(2 * sizeof(char*));
    inp.sqlCondInp.value[0] = strdup("= '/tempZone/home/rods'");
    inp.sqlCondInp.value[1] = strdup("= 'access_type'");

    std::vector<snowflake_id_t> starting_nodes;
    auto ast = bridge::synthesize_gq2_ast(&inp, &catalog, starting_nodes);

    ResultSet results;
    ASSERT_TRUE(catalog.execute_query(ast, results).ok());

    std::cerr << "Query found " << results.row_count() << " rows." << std::endl;
    for (size_t i = 0; i < results.row_count(); ++i) {
        std::cerr << "  Row " << i << ": DATA_ID=" << results.get_field(i, 0) << ", USER_NAME=" << results.get_field(i, 1) << ", USER_TYPE=" << results.get_field(i, 2) << std::endl;
    }

    EXPECT_GT(results.row_count(), 0) << "Expected to find access row";

    // Cleanup
    free(inp.selectInp.inx);
    free(inp.selectInp.value);
    free(inp.sqlCondInp.inx);
    free(inp.sqlCondInp.value[0]);
    free(inp.sqlCondInp.value[1]);
    free(inp.sqlCondInp.value);
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
