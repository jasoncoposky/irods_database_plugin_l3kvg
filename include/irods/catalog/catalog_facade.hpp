#pragma once

#include <memory>
#include "irods/catalog/catalog_models.hpp"
#include "irods/irods_error.hpp"

#include <variant>

namespace irods::catalog {

    namespace compiler {
        struct ConditionNode;
        struct SelectNode;
        using AstNode = std::variant<ConditionNode, SelectNode>;
    }

    struct Config {
        std::string db_path;
        uint32_t node_id;
        uint32_t shard_count;
        std::string zmq_endpoint;
    };

    class CatalogImpl;

    class CatalogFacade {
    public:
        CatalogFacade();
        ~CatalogFacade();

        // Initialization
        irods::error init(const Config& cfg);
        irods::error register_data_object(const data_object& obj, data_id_t& out_id);
        irods::error delete_data_object(data_id_t id);

        // Replica Operations
        irods::error register_replica(const replica& repl);

        // Collection Operations
        irods::error register_collection(const collection& coll, coll_id_t& out_id);

        // ACL Operations
        irods::error set_access(uint64_t user_id, uint64_t target_id, std::string_view level);
        
        // Metadata ACL Pushdown
        irods::error add_metadata_with_acl(data_id_t object_id, const avu& metadata, const std::vector<uint64_t>& allowed_groups);

        // Query Operations
        irods::error execute_query(const std::vector<compiler::AstNode>& ast, std::vector<std::vector<std::string>>& results);

    private:
        std::unique_ptr<CatalogImpl> pImpl_;
    };

} // namespace irods::catalog
