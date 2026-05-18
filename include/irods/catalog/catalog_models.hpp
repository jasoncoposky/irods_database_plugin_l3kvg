#pragma once

#include <string>
#include <vector>
#include <optional>
#include <cstdint>

namespace irods::catalog {

    using data_id_t = uint64_t;
    using coll_id_t = uint64_t;
    using user_id_t = uint64_t;
    using resc_id_t = uint64_t;

    struct data_object {
        data_id_t id = 0;
        coll_id_t coll_id = 0;
        std::string name;
        std::string owner_name;
        std::string owner_zone;
        std::string type;
        uint64_t size = 0;
        std::string version;
        std::string mode;
        std::string expiry;
        std::string status;
        std::string checksum;
        std::string comments;
        std::string create_ts;
        std::string modify_ts;
    };

    struct replica {
        data_id_t data_id = 0;
        uint32_t replica_number = 0;
        resc_id_t resource_id = 0;
        std::string physical_path;
        std::string resc_hier;
        std::string status;
        std::string checksum;
        std::string modify_ts;
        std::string access_time;
    };

    struct collection {
        coll_id_t id = 0;
        coll_id_t parent_id = 0;
        std::string name;
        std::string owner_name;
        std::string owner_zone;
        std::string inheritance;
        std::string type;
        std::string info1;
        std::string info2;
        std::string comments;
        std::string create_ts;
        std::string modify_ts;
    };

    struct user {
        user_id_t id = 0;
        std::string name;
        std::string zone;
        std::string type; // rodsuser, rodsadmin, etc.
        std::string dn;
        std::string info;
        std::string comment;
        std::string create_ts;
        std::string modify_ts;
    };

    struct resource {
        resc_id_t id = 0;
        std::string name;
        std::string type;
        std::string location;
        std::string vault_path;
        std::string context;
        std::string comments;
        int64_t free_space = 0;
        int status = 1; // 1=up, 0=down
        std::string create_ts;
        std::string modify_ts;
    };

    struct avu {
        std::string attribute;
        std::string value;
        std::string units;
    };

    struct zone {
        std::string name;
        std::string type;
        std::string connection;
        std::string comment;
    };

} // namespace irods::catalog
