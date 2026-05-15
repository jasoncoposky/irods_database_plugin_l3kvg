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
        data_id_t id;
        coll_id_t coll_id;
        std::string name;
        std::string logical_path;
        uint64_t size;
        std::string owner_name;
        std::string owner_zone;
        std::string create_ts;
        std::string modify_ts;
        
        // Extended fields for production
        std::string checksum;
        uint32_t repl_num;
        std::string resc_name;
        std::string path;
        std::string resc_hier;
        uint64_t resc_id;
        std::string repl_status;
    };

    struct replica {
        data_id_t data_id;
        uint32_t replica_number;
        resc_id_t resource_id;
        std::string physical_path;
        std::string resource_name;
        std::string status;
        std::string checksum;
    };

    struct collection {
        coll_id_t id;
        coll_id_t parent_id;
        std::string name;
        std::string owner_name;
        std::string owner_zone;
        std::string create_ts;
        std::string modify_ts;
    };

    struct user {
        user_id_t id;
        std::string name;
        std::string zone;
        std::string type; // rodsuser, rodsadmin, etc.
    };

    struct resource {
        resc_id_t id;
        std::string name;
        std::string type;
        std::string location;
        std::string vault_path;
        std::string context;
        std::string parent_id;
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
    };

    enum class DbError {
        Success = 0,
        EngineFault,
        NotFound,
        Collision,
        PermissionDenied,
        InvalidInput
    };

} // namespace irods::catalog
