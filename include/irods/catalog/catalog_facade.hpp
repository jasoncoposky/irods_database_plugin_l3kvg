#pragma once

#include <memory>
#include "irods/catalog/catalog_models.hpp"
#include "irods/irods_error.hpp"

#include <variant>

#include "L3KVG/Query.hpp"

namespace irods::catalog {

    namespace compiler {
        struct ConditionNode;
        struct SelectNode;
        using AstNode = std::variant<ConditionNode, SelectNode>;
    }

    struct ResultSet {
        std::vector<l3kvg::Query::ResultRow> rows;
        
        size_t row_count() const { return rows.size(); }
        std::string_view get_field(size_t row, std::string_view key) const {
            auto it = rows[row].fields.find(std::string(key));
            return (it != rows[row].fields.end()) ? it->second : std::string_view{};
        }
    };

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

        // Identity Operations
        irods::error register_user(const user& usr, user_id_t& out_id);
        irods::error check_auth(std::string_view user_name, std::string_view zone, int& user_priv);
        irods::error add_user_to_group(user_id_t user_id, user_id_t group_id);
        irods::error remove_user_from_group(user_id_t user_id, user_id_t group_id);
        irods::error set_user_property(user_id_t user_id, std::string_view prop, std::string_view value);

        // ACL Operations
        irods::error set_access(uint64_t user_id, uint64_t target_id, std::string_view level);
        
        // Metadata ACL Pushdown
        irods::error add_metadata_with_acl(data_id_t object_id, const avu& metadata, const std::vector<uint64_t>& allowed_groups);

        // Query Operations
        irods::error execute_query(const std::vector<compiler::AstNode>& ast, ResultSet& results);

    private:
        std::unique_ptr<CatalogImpl> pImpl_;
    };

} // namespace irods::catalog
