#include "irods/catalog/catalog_facade.hpp"
#include <iostream>
#include <vector>
#include <thread>
#include <chrono>
#include <unistd.h>

using namespace irods::catalog;

int main() {
    Config cfg;
    cfg.db_path = "/var/lib/irods/l3kvg_db";
    cfg.node_id = 1;
    cfg.cluster_id = 521;
    cfg.shard_count = 1;
    cfg.zmq_endpoint = "tcp://127.0.0.1:5556";

    CatalogFacade catalog;
    if (!catalog.init(cfg, "tempZone").ok()) {
        std::cerr << "Failed to init catalog\n";
        return 1;
    }

    std::cout << "Connecting to L3KVG at " << cfg.zmq_endpoint << "...\n";
    std::this_thread::sleep_for(std::chrono::seconds(1));

    // Bootstrap Zone and Admin
    catalog.bootstrap_catalog("tempZone", "rods");
    std::cout << "Zone and Admin bootstrapped.\n";

    // Set 'own' access for roots
    catalog.set_access("rods", "tempZone", "/", "own", false);
    catalog.set_access("rods", "tempZone", "/tempZone", "own", false);

    std::cout << "Creating collection hierarchy...\n";
    
    collection home;
    home.id = 10002;
    home.name = "/tempZone/home";
    home.parent_name = "/tempZone";
    home.owner_name = "rods";
    home.owner_zone = "tempZone";
    home.type = "";
    home.parent_id = 1; 
    home.create_ts = "01748200000";
    home.modify_ts = "01748200000";
    catalog.register_collection(home, home.id);
    catalog.set_access("rods", "tempZone", "/tempZone/home", "own", false);

    collection rods_home;
    rods_home.id = 10003;
    rods_home.name = "/tempZone/home/rods";
    rods_home.parent_name = "/tempZone/home";
    rods_home.owner_name = "rods";
    rods_home.owner_zone = "tempZone";
    rods_home.type = "";
    rods_home.parent_id = 10002;
    rods_home.create_ts = "01748200000";
    rods_home.modify_ts = "01748200000";
    catalog.register_collection(rods_home, rods_home.id);
    catalog.set_access("rods", "tempZone", "/tempZone/home/rods", "own", false);

    collection rods_trash;
    rods_trash.id = 10008;
    rods_trash.name = "/tempZone/trash/home/rods";
    rods_trash.parent_name = "/tempZone/trash/home";
    rods_trash.owner_name = "rods";
    rods_trash.owner_zone = "tempZone";
    rods_trash.type = "";
    rods_trash.parent_id = 5;
    rods_trash.create_ts = "01748200000";
    rods_trash.modify_ts = "01748200000";
    catalog.register_collection(rods_trash, rods_trash.id);
    catalog.set_access("rods", "tempZone", "/tempZone/trash/home/rods", "own", false);

    char hostname[1024];
    gethostname(hostname, 1024);

    std::cout << "Registering resources for " << hostname << "...\n";
    resource resc;
    resc.id = 40001;
    resc.name = "demoResc";
    resc.type = "unixfilesystem";
    resc.location = hostname;
    resc.vault_path = "/var/lib/irods/Vault";
    resc.status = 0;
    resc.create_ts = "01748200000";
    resc.modify_ts = "01748200000";
    resc_id_t out_resc_id;
    catalog.register_resource(resc, out_resc_id);

    std::cout << "Hierarchy created. Waiting for ZMQ to flush...\n";
    std::this_thread::sleep_for(std::chrono::seconds(2));

    std::cout << "Bootstrap complete.\n";
    return 0;
}
