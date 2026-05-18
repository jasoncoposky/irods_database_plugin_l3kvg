#pragma once

#include <memory>
#include <vector>
#include <string>
#include <string_view>
#include <map>
#include <unordered_map>
#include "irods/catalog/catalog_models.hpp"
#include "irods/irods_error.hpp"
#include "irods/private/genquery2_ast_types.hpp"
#include "L3KVG/Query.hpp"

namespace irods::catalog {

    struct ResultSet {
        std::vector<l3kvg::Query::ResultRow> rows;
        size_t row_count() const { return rows.size(); }
        std::string_view get_field(size_t row, std::string_view key) const {
            if (row >= rows.size()) return "";
            auto it = rows[row].fields.find(std::string(key));
            return (it == rows[row].fields.end()) ? "" : it->second;
        }
        std::string_view get_field(size_t row, size_t col_idx) const {
            return get_field(row, std::to_string(col_idx));
        }
    };

    struct FederatedZone {
        std::string name;
        uint16_t id;
        std::string endpoint;
    };

    struct Config {
        std::string db_path;
        uint32_t node_id;
        uint32_t shard_count;
        std::string zmq_endpoint;
        std::vector<FederatedZone> federation;
    };

    class CatalogImpl;

    class CatalogFacade {
    public:
        CatalogFacade();
        ~CatalogFacade();

        irods::error init(const Config& cfg);
        irods::error bootstrap_catalog(std::string_view zone_name, std::string_view admin_name);
        irods::error bootstrap_federation(const std::vector<FederatedZone>& peers);

        // Data Object Operations
        irods::error register_data_object(const data_object& obj, data_id_t& out_id);
        irods::error delete_data_object(data_id_t id);
        irods::error rename_data_object(data_id_t obj_id, std::string_view new_name);
        irods::error move_data_object(data_id_t obj_id, coll_id_t target_coll_id);
        irods::error modify_data_object(data_id_t obj_id, std::string_view prop, std::string_view value);

        // Replica Operations
        irods::error register_replica(const replica& repl);
        irods::error unregister_replica(data_id_t data_id, uint32_t repl_num);
        irods::error update_replica_access_time(data_id_t data_id, uint32_t repl_num, std::string_view time);

        // Collection Operations
        irods::error register_collection(const collection& coll, coll_id_t& out_id);
        irods::error rename_collection(std::string_view old_name, std::string_view new_name);
        irods::error delete_collection(coll_id_t coll_id);
        irods::error modify_collection(coll_id_t coll_id, std::string_view prop, std::string_view value);

        // Resource Operations
        irods::error register_resource(const resource& resc, resc_id_t& out_id);
        irods::error modify_resource(resc_id_t resc_id, std::string_view prop, std::string_view value);
        irods::error delete_resource(resc_id_t resc_id);
        irods::error resolve_resource_name(std::string_view name, resc_id_t& out_id);
        irods::error update_resource_object_count(resc_id_t resc_id, int delta);

        // Identity Operations
        irods::error register_user(const user& usr, user_id_t& out_id);
        irods::error delete_user(user_id_t user_id);
        irods::error modify_user(user_id_t user_id, std::string_view prop, std::string_view value);
        irods::error check_auth(std::string_view user_name, std::string_view zone, int& user_priv);
        irods::error check_auth_credentials(std::string_view username, std::string_view zone, std::string_view password, bool& correct);
        irods::error add_user_to_group(user_id_t user_id, user_id_t group_id);
        irods::error remove_user_from_group(user_id_t user_id, user_id_t group_id);

        // ACL Operations
        irods::error set_access(uint64_t user_id, uint64_t target_id, std::string_view level, bool recursive);
        irods::error check_permission(uint64_t user_id, uint64_t target_id, std::string_view level, bool& allowed);

        // Metadata (AVU) Operations
        irods::error add_avu_metadata(std::string_view type, std::string_view target_id, const avu& metadata);
        irods::error delete_avu_metadata(std::string_view type, std::string_view target_id, const avu& metadata);
        irods::error modify_avu_metadata(std::string_view type, std::string_view target_id, const avu& old_avu, const avu& new_avu);
        irods::error copy_avu_metadata(std::string_view src_type, std::string_view src_id, std::string_view dst_type, std::string_view dst_id);
        irods::error set_avu_metadata(std::string_view type, std::string_view target_id, const avu& metadata);

        // Zone Operations
        irods::error register_zone(const zone& z);
        irods::error modify_zone(std::string_view name, std::string_view prop, std::string_view value);
        irods::error delete_zone(std::string_view name);

        // Token & Quota Operations
        irods::error register_token(std::string_view name, std::string_view value, std::string_view namespace_str);
        irods::error delete_token(std::string_view name, std::string_view namespace_str);
        irods::error set_quota(std::string_view user_name, std::string_view resc_name, int64_t limit);
        irods::error check_quota(std::string_view user_name, std::string_view resc_name, int64_t& usage, int64_t& limit);

        // Query Operations
        irods::error execute_query(const irods::experimental::genquery2::select& ast, ResultSet& results);
        irods::error get_next_sequence_value(std::string_view seq_name, uint64_t& out_val);

    private:
        std::unique_ptr<CatalogImpl> pImpl_;
    };

} // namespace irods::catalog
