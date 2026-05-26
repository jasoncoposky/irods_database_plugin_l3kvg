#include "irods/catalog/catalog_facade.hpp"
#include "irods/catalog/binary_key.hpp"
#include <iostream>
#include <thread>
#include <chrono>

using namespace irods::catalog;

int main() {
    Config cfg;
    cfg.db_path = "/home/darkfell/dev/l3kvg_db";
    cfg.node_id = 1;
    cfg.shard_count = 1;
    cfg.zmq_endpoint = "tcp://127.0.0.1:5555";
    cfg.cluster_id = SnowflakeID::calculate_cluster_id("tempZone");

    CatalogFacade catalog;
    if (!catalog.init(cfg).ok()) {
        std::cerr << "Failed to init catalog" << std::endl;
        return 1;
    }

    std::cout << "Connecting to L3KVG at " << cfg.zmq_endpoint << "..." << std::endl;

    // 1. Bootstrap Zone and Admin
    if (!catalog.bootstrap_catalog("tempZone", "rods").ok()) {
        std::cerr << "Failed to bootstrap catalog" << std::endl;
        return 1;
    }
    std::cout << "Zone and Admin bootstrapped." << std::endl;

    // 2. Create Collection Hierarchy
    std::cout << "Creating collection hierarchy..." << std::endl;
    collection root_coll;
    root_coll.id = 10001;
    root_coll.name = "/tempZone";
    root_coll.owner_name = "rods";
    root_coll.owner_zone = "tempZone";
    root_coll.type = "local";
    root_coll.create_ts = "01748200000";
    root_coll.modify_ts = "01748200000";
    catalog.register_collection(root_coll, root_coll.id);

    collection home;
    home.id = 10002;
    home.name = "/tempZone/home";
    home.owner_name = "rods";
    home.owner_zone = "tempZone";
    home.type = "local";
    home.create_ts = "01748200000";
    home.modify_ts = "01748200000";
    catalog.register_collection(home, home.id);

    collection rods_home;
    rods_home.id = 10003;
    rods_home.name = "/tempZone/home/rods";
    rods_home.owner_name = "rods";
    rods_home.owner_zone = "tempZone";
    rods_home.type = "local";
    rods_home.create_ts = "01748200000";
    rods_home.modify_ts = "01748200000";
    catalog.register_collection(rods_home, rods_home.id);

    // 3. Register Data Object
    data_object obj;
    obj.id = 20001;
    obj.name = "test_file.txt";
    obj.coll_id = rods_home.id;
    obj.owner_name = "rods";
    obj.owner_zone = "tempZone";
    obj.size = 1024;
    obj.type = "generic";
    obj.create_ts = "01748200000";
    obj.modify_ts = "01748200000";
    catalog.register_data_object(obj, obj.id);

    // 4. Register Resource
    std::cout << "Registering resources..." << std::endl;
    resource resc;
    resc.id = 40001;
    resc.name = "demoResc";
    resc.type = "unixfilesystem";
    resc.status = 1;
    catalog.register_resource(resc, resc.id);

    // 5. Register Replica
    replica r;
    r.data_id = obj.id;
    r.replica_number = 0;
    r.resource_id = 40001;
    r.physical_path = "/var/lib/irods/Vault/test_file.txt";
    r.resc_hier = "demoResc";
    r.status = "1";
    r.modify_ts = "01748200000";
    catalog.register_replica(r);

    std::cout << "Hierarchy created. Waiting for ZMQ to flush..." << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(1));

    std::cout << "Bootstrap complete." << std::endl;
    return 0;
}
