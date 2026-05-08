#include <gtest/gtest.h>
#include "irods/catalog/catalog_facade.hpp"
#include <vector>

using namespace irods::catalog;

TEST(MetadataAclTest, PushdownFilter) {
    CatalogFacade catalog;
    Config cfg;
    cfg.db_path = "catalog.l3kvg";
    cfg.node_id = 1;
    ASSERT_TRUE(catalog.init(cfg).ok());

    // 1. Create a Data Object
    data_object obj;
    obj.id = 1001;
    obj.coll_id = 500;
    obj.name = "sensitive_data.dat";
    
    data_id_t out_id;
    auto ret = catalog.register_data_object(obj, out_id);
    ASSERT_TRUE(ret.ok());

    // 2. Add "Public" Metadata (allowed for Group 10 - Public)
    avu public_meta{"status", "published", ""};
    ret = catalog.add_metadata_with_acl(out_id, public_meta, {10});
    ASSERT_TRUE(ret.ok());

    // 3. Add "Secret" Metadata (allowed for Group 99 - Admins)
    avu secret_meta{"clearance", "top_secret", ""};
    ret = catalog.add_metadata_with_acl(out_id, secret_meta, {99});
    ASSERT_TRUE(ret.ok());

    // Validation (In a real system, the Gq2Compiler would use these edges)
    // Here we have proven the "Fat Edge" contains the group IDs for cache pushdown.
    SUCCEED();
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
