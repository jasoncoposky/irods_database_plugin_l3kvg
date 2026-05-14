#include <gtest/gtest.h>
#include "irods/catalog/catalog_facade.hpp"
#include "irods/catalog/gq2_compiler.hpp"
#include "irods/rodsGenQuery.h"
#include "L3KVG/Engine.hpp"
#include "engine/store.hpp"

using namespace irods::catalog;

TEST(AdminQueryTest, ZoneListing) {
    CatalogFacade catalog;
    Config cfg;
    cfg.db_path = "admin.l3kvg";
    cfg.node_id = 1;
    ASSERT_TRUE(catalog.init(cfg).ok());

    // 1. Setup: Zone with multiple users
    zone z{"tempZone", "local", ""};
    catalog.register_zone(z);

    user u1{101, "alice", "tempZone", "rodsuser"};
    user u2{102, "bob", "tempZone", "rodsuser"};
    user_id_t out_id;
    catalog.register_user(u1, out_id);
    catalog.register_user(u2, out_id);

    catalog.get_engine()->get_store()->wait_all_shards();

    // 2. Query: SELECT USER_NAME WHERE ZONE_NAME = 'tempZone'
    // This tests the Zone -> User traversal we just added.
    std::vector<compiler::AstNode> ast;
    
    compiler::SelectNode sel;
    sel.columns.push_back(COL_USER_NAME);
    ast.push_back(sel);

    compiler::ConditionNode cond;
    cond.column = COL_ZONE_NAME;
    cond.value = "tempZone";
    ast.push_back(cond);

    ResultSet results;
    ASSERT_TRUE(catalog.execute_query(ast, results).ok());
    
    // 3. Validate
    ASSERT_EQ(results.row_count(), 2);
    // Sort or check both to be safe
    std::set<std::string_view> names;
    names.insert(results.get_field(0, "User.name"));
    names.insert(results.get_field(1, "User.name"));
    EXPECT_TRUE(names.contains("alice"));
    EXPECT_TRUE(names.contains("bob"));
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
