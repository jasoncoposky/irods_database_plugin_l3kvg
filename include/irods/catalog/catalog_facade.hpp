#pragma once

#include <memory>
#include <vector>
#include <string>
#include <string_view>
#include <map>
#include <unordered_map>
#include "irods/catalog/catalog_models.hpp"
#include "irods/catalog/binary_key.hpp"
#include "irods/irods_error.hpp"
#include "irods/private/genquery2_ast_types.hpp"
#include "irods/catalog/gq2_compiler.hpp"
#include "irods/atomic_apply_database_operations.hpp"
#include "L3KVG/Query.hpp"
#include "L3KVG/Settings.hpp"
#include "buffer.hpp"

namespace irods::catalog {

    struct ResultSet {
        std::vector<l3kvg::Query::ResultRow> rows;
        size_t row_count() const { return rows.size(); }
        std::string_view get_field(size_t row, std::string_view key) const {
            if (row >= rows.size()) return "";
            auto it = rows[row].fields.find(std::string(key));
            if (it == rows[row].fields.end()) return "";
            return it->second;
        }
        std::string_view get_field(size_t row, size_t col_idx) const {
            if (row < rows.size()) {
                if (col_idx < rows[row].projected_values.size()) {
                    return rows[row].projected_values[col_idx];
                }
            }
            if (col_idx < 64) {
                static const std::string idx_keys[] = {
                    "idx_0", "idx_1", "idx_2", "idx_3", "idx_4", "idx_5", "idx_6", "idx_7",
                    "idx_8", "idx_9", "idx_10", "idx_11", "idx_12", "idx_13", "idx_14", "idx_15",
                    "idx_16", "idx_17", "idx_18", "idx_19", "idx_20", "idx_21", "idx_22", "idx_23",
                    "idx_24", "idx_25", "idx_26", "idx_27", "idx_28", "idx_29", "idx_30", "idx_31",
                    "idx_32", "idx_33", "idx_34", "idx_35", "idx_36", "idx_37", "idx_38", "idx_39",
                    "idx_40", "idx_41", "idx_42", "idx_43", "idx_44", "idx_45", "idx_46", "idx_47",
                    "idx_48", "idx_49", "idx_50", "idx_51", "idx_52", "idx_53", "idx_54", "idx_55",
                    "idx_56", "idx_57", "idx_58", "idx_59", "idx_60", "idx_61", "idx_62", "idx_63"
                };
                if (row >= rows.size()) return "";
                auto it = rows[row].fields.find(idx_keys[col_idx]);
                if (it == rows[row].fields.end()) return "";
                return it->second;
            }
            std::string key = "idx_" + std::to_string(col_idx);
            return get_field(row, key);
        }
    };

    struct AclEntry {
        std::string user_name;
        std::string zone_name;
        std::string access_name;
        std::string user_type;
    };

    struct FederatedZone {
        std::string name;
        uint16_t id;
        std::string endpoint;
    };

    struct Config {
        std::string db_path;
        uint32_t node_id;
        uint16_t cluster_id;
        uint32_t shard_count;
        std::string zmq_endpoint;
        std::string default_resc;
        std::string default_resc_vault;
        std::string admin_user;
        std::vector<FederatedZone> federation;
    };

    class CatalogImpl;

    class CatalogFacade {
    public:
        CatalogFacade();
        ~CatalogFacade();

        irods::error init(const Config& cfg, std::string_view zone_name, const l3kvg::Settings& settings = {});
        irods::error bootstrap_catalog(std::string_view zone_name, std::string_view admin_name);
        irods::error bootstrap_federation(const std::vector<FederatedZone>& peers);

        // Data Object Operations
        irods::error register_data_object(const data_object& obj, data_id_t& out_id, const replica* initial_repl = nullptr);
        irods::error delete_data_object(data_id_t id);
        irods::error rename_data_object(data_id_t obj_id, std::string_view new_name);
        irods::error move_data_object(data_id_t obj_id, coll_id_t target_coll_id);
        irods::error modify_data_object(data_id_t obj_id, std::string_view prop, std::string_view value);
        irods::error rename_object(uint64_t obj_id, std::string_view new_name);
        irods::error move_object(uint64_t obj_id, coll_id_t target_coll_id);

        // Replica Operations
        irods::error register_replica(const replica& repl);
        irods::error unregister_replica(data_id_t data_id, uint32_t repl_num);
        irods::error update_replica_access_time(data_id_t data_id, uint32_t repl_num, std::string_view time);
        irods::error modify_replicas_for_data_object(data_id_t obj_id, uint32_t repl_num, std::string_view resc_hier, const std::vector<std::pair<std::string, std::string>>& updates, bool all_repl_status, bool all_replicas = false);
        uint32_t get_next_replica_number(data_id_t data_id);

        // Collection Operations
        irods::error register_collection(const collection& coll, coll_id_t& out_id);
        irods::error rename_collection(std::string_view old_name, std::string_view new_name);
        irods::error delete_collection(coll_id_t coll_id);
        irods::error is_collection_empty(coll_id_t coll_id, bool& is_empty);
        irods::error modify_collection(coll_id_t coll_id, std::string_view prop, std::string_view value);
        irods::error get_collection_subtree_ids(snowflake_id_t coll_sid, std::vector<snowflake_id_t>& out_ids);
        irods::error get_child_collection_ids(snowflake_id_t parent_sid, std::string_view parent_path, std::vector<snowflake_id_t>& out_ids);

        // Resource Operations
        irods::error register_resource(const resource& resc, resc_id_t& out_id);
        irods::error modify_resource(snowflake_id_t sid, std::string_view prop, std::string_view value);
        irods::error delete_resource(snowflake_id_t sid);
        irods::error resolve_resource_name(std::string_view name, snowflake_id_t& out_id);
        irods::error resolve_user_name(std::string_view name, snowflake_id_t& out_id);
        irods::error get_hierarchy_for_resource(std::string_view name, std::string& out_hier);
        irods::error update_resource_object_count(resc_id_t resc_id, int delta);
        irods::error add_child_resource(std::string_view parent_name, std::string_view child_name, std::string_view context);
        irods::error remove_child_resource(std::string_view parent_name, std::string_view child_name);

        // Identity Operations
        irods::error register_user(const user& usr, user_id_t& out_id);
        irods::error delete_user(std::string_view user_name, std::string_view zone = "");
        irods::error modify_user(std::string_view user_name, std::string_view prop, std::string_view value, std::string_view zone = "");
        snowflake_id_t resolve_user(std::string_view user_name, std::string_view zone = "");
        irods::error check_auth(std::string_view user_name, std::string_view zone, int& user_priv);
        irods::error get_user_password_and_priv(std::string_view user_name, std::string_view zone, std::string& out_pw, int& out_priv);
        irods::error check_auth_credentials(std::string_view username, std::string_view zone, std::string_view password, bool& correct);
        irods::error add_user_to_group(std::string_view user_name, std::string_view zone, std::string_view group_name);
        irods::error remove_user_from_group(std::string_view user_name, std::string_view zone, std::string_view group_name);

        // ACL Operations
        irods::error set_access(std::string_view user_name, std::string_view zone, std::string_view target_path, std::string_view level, bool recursive);
        irods::error check_permission(snowflake_id_t user_id, snowflake_id_t target_id, std::string_view level, bool& allowed, bool check_parents = true);
        irods::error check_permission_to_modify_data_object(snowflake_id_t user_id, snowflake_id_t target_id, bool& allowed);

        // Metadata (AVU) Operations
        irods::error add_avu_metadata(std::string_view type, std::string_view target_id, const avu& metadata);
        irods::error delete_avu_metadata(std::string_view type, std::string_view target_id, const avu& metadata, int option = 0);
        irods::error modify_avu_metadata(std::string_view type, std::string_view target_id, const avu& old_avu, const avu& new_avu);
        irods::error copy_avu_metadata(std::string_view src_type, std::string_view src_id, std::string_view dst_type, std::string_view dst_id);
        irods::error set_avu_metadata(std::string_view type, std::string_view target_id, const avu& metadata);

        // Path Resolution
        irods::error resolve_path(std::string_view path, snowflake_id_t& out_id, EntityType& out_type);
        snowflake_id_t resolve_target_entity_sid(std::string_view type, std::string_view target_id_or_name);

        // --- Grid Configuration ---

        irods::error register_zone(const zone& z);
        irods::error modify_zone(std::string_view name, std::string_view prop, std::string_view value);
        irods::error delete_zone(std::string_view name);

        // Ticket Operations
        irods::error create_ticket(uint64_t ticket_id, std::string_view ticket_string, std::string_view ticket_type, snowflake_id_t target_sid, EntityType target_type, snowflake_id_t user_sid, std::string_view user_name, std::string_view target_path = "");
        irods::error delete_ticket(std::string_view ticket_string, std::string_view calling_user = "", bool is_admin = false);
        irods::error modify_ticket(std::string_view ticket_string, std::string_view op, std::string_view arg1, std::string_view arg2 = "", std::string_view calling_user = "", bool is_admin = false);
        irods::error get_ticket_restrictions(std::string_view ticket_id_or_str, std::string_view restriction_type, std::vector<std::pair<std::string, std::string>>& out_restrictions);
        irods::error validate_ticket(std::string_view ticket_str, std::string_view client_user, std::string_view client_host, std::string* out_target_path = nullptr, std::string* out_target_type = nullptr);
        irods::error check_ticket_access(std::string_view ticket_str, snowflake_id_t obj_sid, std::string_view access_type, std::string_view client_user, std::string_view client_host);
        irods::error update_ticket_write_bytes(std::string_view ticket_str, snowflake_id_t obj_sid, int64_t bytes);
        irods::error increment_ticket_uses(std::string_view ticket_str, uint64_t data_id);
        void reset_ticket_session_state();

        // Token & Quota Operations
        irods::error register_token(std::string_view name, std::string_view value, std::string_view namespace_str);
        irods::error delete_token(std::string_view name, std::string_view namespace_str);
        irods::error set_quota(std::string_view user_name, std::string_view resc_name, int64_t limit);
        irods::error check_quota(std::string_view user_name, std::string_view resc_name, int64_t& usage, int64_t& limit);
        irods::error calculate_usage(std::string_view user_name, std::string_view resc_name, int64_t& usage);
        irods::error set_logical_quota(std::string_view coll_name, int64_t limit);
        irods::error check_logical_quota(std::string_view coll_name, int64_t& usage, int64_t& limit);
        irods::error calculate_logical_usage(std::string_view coll_name, int64_t& usage);

        // Server Operations
        irods::error register_server_load(std::string_view host, int load);
        irods::error purge_server_load(std::string_view host);

        // Grid Config Operations
        irods::error set_grid_configuration_value(std::string_view key, std::string_view value);
        irods::error get_grid_configuration_value(std::string_view key, std::string& out_value);

        // Rule Operations
        irods::error register_rule_execution(const rule_exec& re, uint64_t& out_id);
        irods::error get_rule_execution(uint64_t id, rule_exec& out_re);
        irods::error lock_rule_execution(uint64_t id, std::string_view lock_host, int lock_host_pid);
        irods::error unlock_rule_execution(uint64_t id);
        irods::error delete_rule_execution(uint64_t id);

        // Specific Query Operations
        irods::error register_specific_query(std::string_view alias, std::string_view sql);
        irods::error delete_specific_query(std::string_view alias);
        irods::error has_specific_query(std::string_view alias_or_sql, bool& out_has);
        irods::error get_collection_acls(std::string_view coll_name, std::vector<AclEntry>& out_acls);

        // Query Operations
        irods::error execute_query(const irods::experimental::genquery2::select& ast, ResultSet& results, const std::vector<uint64_t>& starting_nodes = {}, std::string_view root_type = "", const irods::experimental::genquery2::options* opts = nullptr);
        irods::error execute_dml(const compiler::DmlPlan& plan, lite3cpp::Buffer& result);
        irods::error apply_atomic_operations(const std::vector<irods::experimental::dml::operation_type>& ops);
        irods::error get_next_sequence_value(std::string_view seq_name, uint64_t& out_val);
        snowflake_id_t make_id(EntityType type, uint64_t irods_id);
        snowflake_id_t resolve_id_from_index(EntityType type, std::string_view attr, std::string_view value);
        snowflake_id_t get_zone_id(std::string_view zname = "") const;
        const std::string& get_local_zone_name() const;

        l3kvg::RemoteL3KVClient* get_client() const;
        uint16_t get_cluster_id() const;

    private:
        std::unique_ptr<CatalogImpl> pImpl_;
    };

} // namespace irods::catalog
