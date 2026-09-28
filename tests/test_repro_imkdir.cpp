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

TEST_F(GenQueryCompatibilityTest, ImkdirFailsWhenFileAlreadyExists) {
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

    // 1. Register Parent Collection
    coll_id_t parent_id;
    collection parent;
    parent.id = 200;
    parent.name = "/tempZone/home/rods";
    parent.owner_name = "rods";
    parent.owner_zone = "tempZone";
    ASSERT_TRUE(catalog.register_collection(parent, parent_id).ok());

    // 2. Register Data Object under parent
    data_id_t data_id;
    data_object obj;
    obj.id = 2001;
    obj.coll_id = 200;
    obj.name = "testfile";
    obj.full_path = "/tempZone/home/rods/testfile";
    obj.owner_name = "rods";
    obj.owner_zone = "tempZone";
    ASSERT_TRUE(catalog.register_data_object(obj, data_id).ok());

    // 3. Attempt to register collection with same path as data object -> MUST FAIL with CAT_NAME_EXISTS_AS_DATAOBJ (-834000)
    coll_id_t coll_id;
    collection coll_conflict;
    coll_conflict.id = 201;
    coll_conflict.parent_id = 200;
    coll_conflict.name = "/tempZone/home/rods/testfile";
    coll_conflict.parent_name = "/tempZone/home/rods";
    coll_conflict.owner_name = "rods";
    coll_conflict.owner_zone = "tempZone";
    auto ret = catalog.register_collection(coll_conflict, coll_id);
    EXPECT_FALSE(ret.ok());
    EXPECT_EQ(ret.code(), CAT_NAME_EXISTS_AS_DATAOBJ);

    // 4. Attempt to register existing collection again -> MUST FAIL with CATALOG_ALREADY_HAS_ITEM_BY_THAT_NAME (-809000)
    collection coll_dup;
    coll_dup.id = 202;
    coll_dup.name = "/tempZone/home/rods";
    coll_dup.owner_name = "rods";
    coll_dup.owner_zone = "tempZone";
    auto ret_dup = catalog.register_collection(coll_dup, coll_id);
    EXPECT_FALSE(ret_dup.ok());
    EXPECT_EQ(ret_dup.code(), CATALOG_ALREADY_HAS_ITEM_BY_THAT_NAME);

    // 5. Attempt to register data object with same path as existing collection -> MUST FAIL with CAT_NAME_EXISTS_AS_COLLECTION (-835000)
    data_object obj_conflict;
    obj_conflict.id = 2002;
    obj_conflict.coll_id = 200;
    obj_conflict.name = "rods";
    obj_conflict.full_path = "/tempZone/home/rods";
    obj_conflict.owner_name = "rods";
    obj_conflict.owner_zone = "tempZone";
    auto ret_obj_conflict = catalog.register_data_object(obj_conflict, data_id);
    EXPECT_FALSE(ret_obj_conflict.ok());
    EXPECT_EQ(ret_obj_conflict.code(), CAT_NAME_EXISTS_AS_COLLECTION);
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
