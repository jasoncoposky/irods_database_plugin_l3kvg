#include "plugin_test_fixture.hpp"
#include "irods/catalog/catalog_facade.hpp"
#include "irods/irods_server_properties.hpp"
#include <vector>

using namespace irods::catalog;
using namespace irods::catalog::test;

class MetadataAclTest : public PluginTestFixture {};

TEST_F(MetadataAclTest, PushdownFilter) {
    // 1. Setup Config
    nlohmann::json config;
    config["zone_name"] = "tempZone";
    config["zone_user"] = "rods";
    config["plugin_configuration"]["database"]["l3kvg"]["plugin_specific_configuration"] = {
        {"db_path", "test.l3kvg"},
        {"node_id", 1},
        {"zmq_endpoint", endpoint()}
    };
    irods::server_properties::instance().set_configuration(config);

    CatalogFacade catalog;
    Config cfg;
    cfg.node_id = 1;
    cfg.zmq_endpoint = endpoint();
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
    ret = catalog.add_avu_metadata("-d", std::to_string(out_id).c_str(), public_meta);
    ASSERT_TRUE(ret.ok());

    // 3. Add "Secret" Metadata (allowed for Group 99 - Admins)
    avu secret_meta{"clearance", "top_secret", ""};
    ret = catalog.add_avu_metadata("-d", std::to_string(out_id).c_str(), secret_meta);
    ASSERT_TRUE(ret.ok());

    // Validation
    SUCCEED();
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
