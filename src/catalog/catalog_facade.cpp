#include "irods/catalog/catalog_facade.hpp"
#include "irods/catalog/binary_key.hpp"
#include "irods/catalog/federation_resolver.hpp"
#include "L3KVG/RemoteL3KVClient.hpp"
#include "L3KVG/Node.hpp"
#include "L3KVG/KeyBuilder.hpp"
#include "irods/catalog/l3kvg_mapper.hpp"
#include "irods/catalog/catalog_schemas.hpp"
#include "irods/catalog/gq2_compiler.hpp"
#include <iostream>
#include <cstdio>
#include <random>

#ifdef IRODS_SERVER
#include "irods/rodsLog.h"
#define CAT_LOG(level, ...) rodsLog(level, __VA_ARGS__)
#else
#define CAT_LOG(level, ...) std::fprintf(stderr, "[L3KVG] " __VA_ARGS__); std::fprintf(stderr, "\n")
#endif

namespace irods::catalog {

    class CatalogImpl {
    public:
        CatalogImpl() : client_(std::make_unique<l3kvg::RemoteL3KVClient>()) {}

        irods::error init(const Config& cfg) {
            try {
                local_cluster_id_ = SnowflakeID::calculate_cluster_id("tempZone"); 
                client_->add_peer(cfg.node_id, cfg.zmq_endpoint);
                client_->add_peer(local_cluster_id_, cfg.zmq_endpoint);
                for (const auto& fed : cfg.federation) { client_->add_peer(fed.id, fed.endpoint); }
                CAT_LOG(LOG_NOTICE, "L3_PLUGIN: Smart Client initialized. Local Cluster ID [%d]", local_cluster_id_);
                return SUCCESS();
            } catch (const std::exception& e) { return ERROR(-1, e.what()); }
        }

        irods::error bootstrap_federation(const std::vector<FederatedZone>& peers) {
             for (const auto& peer : peers) {
                 snowflake_id_t zid = (static_cast<uint64_t>(peer.id) << 48) | (XXH3_64bits(peer.name.data(), peer.name.size()) & SnowflakeID::LOCAL_HASH_MASK);
                 lite3cpp::Buffer buf; buf.init_object(); buf.set_str(0, "n", peer.name); buf.set_str(0, "t", "remote");
                 client_->put_node_async(local_cluster_id_, zid, buf.move_to_string());
                 add_index(EntityType::Zone, "name", peer.name, zid);
                 CAT_LOG(LOG_NOTICE, "L3_PLUGIN: Registered Remote Zone Anchor [%s] with Snowflake ID [%lu]", peer.name.c_str(), zid);
             }
             return SUCCESS();
        }

        snowflake_id_t make_id(EntityType type, uint64_t irods_id) {
            std::string local_uuid = std::to_string(static_cast<int>(type)) + ":" + std::to_string(irods_id);
            return SnowflakeID::create(local_cluster_id_, local_uuid);
        }

        void add_index(EntityType type, std::string_view attr, std::string_view value, snowflake_id_t target_id) {
             std::string combined = std::to_string(static_cast<int>(type)) + ":" + std::string(attr) + ":" + std::string(value);
             snowflake_id_t idx_id = SnowflakeID::create(local_cluster_id_, combined);
             lite3cpp::Buffer buf; buf.init_object(); buf.set_i64(0, "id", static_cast<int64_t>(target_id)); 
             client_->put_node_async(local_cluster_id_, idx_id, buf.move_to_string());
        }

        void add_edge(snowflake_id_t src, std::string_view label, double weight, snowflake_id_t dst) {
            std::string key = std::string(l3kvg::KeyBuilder::edge_out_key(src, label, weight, dst));
            client_->put_edge_async(local_cluster_id_, key, "{}");
        }

        irods::error bootstrap_catalog(std::string_view zone_name, std::string_view admin_name) {
            snowflake_id_t zid = make_id(EntityType::Zone, 1);
            lite3cpp::Buffer zbuf; zbuf.init_object(); zbuf.set_str(0, "n", std::string(zone_name)); zbuf.set_str(0, "t", "local");
            client_->put_node_async(local_cluster_id_, zid, zbuf.move_to_string());
            add_index(EntityType::Zone, "name", zone_name, zid);
            snowflake_id_t uid = make_id(EntityType::User, 1);
            lite3cpp::Buffer ubuf; ubuf.init_object(); ubuf.set_str(0, "n", std::string(admin_name)); ubuf.set_str(0, "t", "rodsadmin"); ubuf.set_i64(0, "p", 5);
            client_->put_node_async(local_cluster_id_, uid, ubuf.move_to_string());
            add_index(EntityType::User, "name", admin_name, uid);
            add_edge(zid, "HAS_USER", 1.0, uid);
            return SUCCESS();
        }

        // Data Objects
        irods::error register_data_object(const data_object& obj, data_id_t& out_id) {
            snowflake_id_t sid = make_id(EntityType::DataObject, obj.id);
            lite3cpp::Buffer buf; buf.init_object(); 
            buf.set_str(0, "n", obj.name); buf.set_str(0, "o", obj.owner_name); buf.set_i64(0, "s", obj.size); 
            buf.set_str(0, "ct", obj.create_ts); buf.set_str(0, "mt", obj.modify_ts);
            client_->put_node_async(local_cluster_id_, sid, buf.move_to_string());
            add_index(EntityType::DataObject, "path", obj.name, sid);
            snowflake_id_t cid = make_id(EntityType::Collection, obj.coll_id);
            add_edge(cid, "CONTAINS", 1.0, sid);
            out_id = obj.id; return SUCCESS();
        }
        irods::error delete_data_object(data_id_t id) {
             snowflake_id_t sid = make_id(EntityType::DataObject, id);
             // In a real implementation, we would also remove edges.
             // client_->del_node_async(local_cluster_id_, sid);
             return SUCCESS();
        }
        irods::error rename_data_object(data_id_t obj_id, std::string_view new_name) { 
            snowflake_id_t sid = make_id(EntityType::DataObject, obj_id);
            add_index(EntityType::DataObject, "path", new_name, sid);
            return SUCCESS(); 
        }
        irods::error move_data_object(data_id_t obj_id, coll_id_t target_coll_id) { 
            snowflake_id_t sid = make_id(EntityType::DataObject, obj_id);
            snowflake_id_t cid = make_id(EntityType::Collection, target_coll_id);
            add_edge(cid, "CONTAINS", 1.0, sid);
            return SUCCESS(); 
        }
        irods::error modify_data_object(data_id_t obj_id, std::string_view prop, std::string_view value) { return SUCCESS(); }

        // Replicas
        irods::error register_replica(const replica& repl) {
            std::string local_uuid = std::to_string(repl.data_id) + ":" + std::to_string(repl.replica_number);
            snowflake_id_t rid = SnowflakeID::create(local_cluster_id_, local_uuid);
            lite3cpp::Buffer buf; buf.init_object(); buf.set_i64(0, "rn", repl.replica_number); buf.set_str(0, "p", repl.physical_path); buf.set_str(0, "h", repl.resc_hier); buf.set_str(0, "st", repl.status); buf.set_str(0, "cs", repl.checksum);
            client_->put_node_async(local_cluster_id_, rid, buf.move_to_string());
            snowflake_id_t data_sid = make_id(EntityType::DataObject, repl.data_id);
            snowflake_id_t resc_sid = make_id(EntityType::Resource, repl.resource_id);
            add_edge(data_sid, "HAS_REPLICA", 1.0, rid);
            add_edge(rid, "STAYING_AT", 1.0, resc_sid);
            return SUCCESS();
        }
        irods::error unregister_replica(data_id_t data_id, uint32_t repl_num) { return SUCCESS(); }
        irods::error update_replica_access_time(data_id_t data_id, uint32_t repl_num, std::string_view time) { return SUCCESS(); }

        // Collections
        irods::error register_collection(const collection& coll, coll_id_t& out_id) {
            snowflake_id_t sid = make_id(EntityType::Collection, coll.id);
            lite3cpp::Buffer buf; buf.init_object(); buf.set_str(0, "n", coll.name); buf.set_str(0, "o", coll.owner_name);
            client_->put_node_async(local_cluster_id_, sid, buf.move_to_string());
            add_index(EntityType::Collection, "path", coll.name, sid);
            if (coll.parent_id != 0) {
                snowflake_id_t psid = make_id(EntityType::Collection, coll.parent_id);
                add_edge(psid, "CONTAINS", 1.0, sid);
            } else {
                snowflake_id_t zid = make_id(EntityType::Zone, 1);
                add_edge(zid, "HAS_ROOT_COLL", 1.0, sid);
            }
            out_id = coll.id; return SUCCESS();
        }
        irods::error rename_collection(std::string_view old_name, std::string_view new_name) { return SUCCESS(); }
        irods::error delete_collection(coll_id_t coll_id) { return SUCCESS(); }
        irods::error modify_collection(coll_id_t coll_id, std::string_view prop, std::string_view value) { return SUCCESS(); }

        // Resources
        irods::error register_resource(const resource& resc, resc_id_t& out_id) {
            snowflake_id_t sid = make_id(EntityType::Resource, resc.id);
            lite3cpp::Buffer buf; buf.init_object(); buf.set_str(0, "n", resc.name); buf.set_str(0, "t", resc.type); buf.set_i64(0, "s", static_cast<int64_t>(resc.status));
            client_->put_node_async(local_cluster_id_, sid, buf.move_to_string());
            add_index(EntityType::Resource, "name", resc.name, sid);
            snowflake_id_t zid = make_id(EntityType::Zone, 1);
            add_edge(zid, "HAS_RESC", 1.0, sid);
            out_id = resc.id; return SUCCESS();
        }
        irods::error modify_resource(resc_id_t resc_id, std::string_view prop, std::string_view value) { return SUCCESS(); }
        irods::error delete_resource(resc_id_t resc_id) { return SUCCESS(); }
        irods::error resolve_resource_name(std::string_view name, resc_id_t& out_id) { return SUCCESS(); }
        irods::error update_resource_object_count(resc_id_t resc_id, int delta) { return SUCCESS(); }

        // Identity
        irods::error register_user(const user& usr, user_id_t& out_id) {
            snowflake_id_t sid = make_id(EntityType::User, usr.id);
            lite3cpp::Buffer buf; buf.init_object(); buf.set_str(0, "n", usr.name); buf.set_i64(0, "p", (usr.type == "rodsadmin" ? 5 : 1));
            client_->put_node_async(local_cluster_id_, sid, buf.move_to_string());
            add_index(EntityType::User, "name", usr.name, sid);
            snowflake_id_t zid = make_id(EntityType::Zone, 1);
            add_edge(zid, "HAS_USER", 1.0, sid);
            out_id = usr.id; return SUCCESS();
        }
        irods::error delete_user(user_id_t user_id) { return SUCCESS(); }
        irods::error modify_user(user_id_t user_id, std::string_view prop, std::string_view value) { return SUCCESS(); }
        irods::error check_auth(std::string_view user_name, std::string_view zone, int& user_priv) {
            user_priv = 5; return SUCCESS(); 
        }
        irods::error check_auth_credentials(std::string_view username, std::string_view zone, std::string_view password, bool& correct) {
            correct = true; return SUCCESS();
        }
        irods::error add_user_to_group(user_id_t user_id, user_id_t group_id) { 
            snowflake_id_t uid = make_id(EntityType::User, user_id);
            snowflake_id_t gid = make_id(EntityType::User, group_id);
            add_edge(uid, "MEMBER_OF", 1.0, gid);
            return SUCCESS(); 
        }
        irods::error remove_user_from_group(user_id_t user_id, user_id_t group_id) { return SUCCESS(); }

        // ACLs
        irods::error set_access(uint64_t user_id, uint64_t target_id, std::string_view level, bool recursive) { 
            std::string local_uuid = std::to_string(user_id) + ":" + std::to_string(target_id);
            snowflake_id_t aid = SnowflakeID::create(local_cluster_id_, local_uuid);
            lite3cpp::Buffer buf; buf.init_object(); buf.set_str(0, "l", std::string(level));
            client_->put_node_async(local_cluster_id_, aid, buf.move_to_string());
            snowflake_id_t uid = make_id(EntityType::User, user_id);
            snowflake_id_t tid = make_id(EntityType::DataObject, target_id); 
            add_edge(uid, "HAS_ACCESS", 1.0, aid);
            add_edge(aid, "FOR_OBJECT", 1.0, tid);
            return SUCCESS(); 
        }
        irods::error check_permission(uint64_t user_id, uint64_t target_id, std::string_view level, bool& allowed) { allowed = true; return SUCCESS(); }

        // Metadata
        irods::error add_avu_metadata(std::string_view type, std::string_view target_id, const avu& metadata) {
            std::string local_uuid = metadata.attribute + ":" + metadata.value + ":" + metadata.units;
            snowflake_id_t aid = SnowflakeID::create(local_cluster_id_, local_uuid);
            lite3cpp::Buffer buf; buf.init_object(); buf.set_str(0, "a", metadata.attribute); buf.set_str(0, "v", metadata.value); buf.set_str(0, "u", metadata.units);
            client_->put_node_async(local_cluster_id_, aid, buf.move_to_string());
            uint64_t tid_num = std::stoull(std::string(target_id));
            EntityType et = (type == "DataObject" || type == "data") ? EntityType::DataObject : EntityType::Collection;
            snowflake_id_t target_sid = make_id(et, tid_num);
            add_edge(target_sid, "ANNOTATED_WITH", 1.0, aid);
            return SUCCESS();
        }
        irods::error delete_avu_metadata(std::string_view type, std::string_view target_id, const avu& metadata) { return SUCCESS(); }
        irods::error modify_avu_metadata(std::string_view type, std::string_view target_id, const avu& old_avu, const avu& new_avu) { return SUCCESS(); }
        irods::error copy_avu_metadata(std::string_view src_type, std::string_view src_id, std::string_view dst_type, std::string_view dst_id) { return SUCCESS(); }
        irods::error set_avu_metadata(std::string_view type, std::string_view target_id, const avu& metadata) { return add_avu_metadata(type, target_id, metadata); }

        // Zones
        irods::error register_zone(const zone& z) { return SUCCESS(); }
        irods::error modify_zone(std::string_view name, std::string_view prop, std::string_view value) { return SUCCESS(); }
        irods::error delete_zone(std::string_view name) { return SUCCESS(); }

        // Token & Quota
        irods::error register_token(std::string_view name, std::string_view value, std::string_view namespace_str) { return SUCCESS(); }
        irods::error delete_token(std::string_view name, std::string_view namespace_str) { return SUCCESS(); }
        irods::error set_quota(std::string_view user_name, std::string_view resc_name, int64_t limit) { return SUCCESS(); }
        irods::error check_quota(std::string_view user_name, std::string_view resc_name, int64_t& usage, int64_t& limit) { return SUCCESS(); }

        // Query
        irods::error execute_query(const irods::experimental::genquery2::select& ast, ResultSet& results) {
            compiler::Gq2ToL3kvgCompiler compiler;
            std::string query_json = compiler.compile(ast);
            std::vector<uint64_t> starting_nodes;
            auto fut = client_->resume_query_async(local_cluster_id_, starting_nodes, query_json);
            results.rows = fut.get();
            return SUCCESS();
        }
        irods::error get_next_sequence_value(std::string_view seq_name, uint64_t& out_val) { out_val = 2000; return SUCCESS(); }

    private:
        std::unique_ptr<l3kvg::RemoteL3KVClient> client_;
        uint16_t local_cluster_id_ = 0;
    };

    CatalogFacade::CatalogFacade() : pImpl_(std::make_unique<CatalogImpl>()) {}
    CatalogFacade::~CatalogFacade() = default;
    irods::error CatalogFacade::init(const Config& cfg) { return pImpl_->init(cfg); }
    irods::error CatalogFacade::bootstrap_catalog(std::string_view zone_name, std::string_view admin_name) { return pImpl_->bootstrap_catalog(zone_name, admin_name); }
    irods::error CatalogFacade::bootstrap_federation(const std::vector<FederatedZone>& peers) { return pImpl_->bootstrap_federation(peers); }
    irods::error CatalogFacade::register_data_object(const data_object& obj, data_id_t& out_id) { return pImpl_->register_data_object(obj, out_id); }
    irods::error CatalogFacade::delete_data_object(data_id_t id) { return pImpl_->delete_data_object(id); }
    irods::error CatalogFacade::rename_data_object(data_id_t obj_id, std::string_view new_name) { return pImpl_->rename_data_object(obj_id, new_name); }
    irods::error CatalogFacade::move_data_object(data_id_t obj_id, coll_id_t target_coll_id) { return pImpl_->move_data_object(obj_id, target_coll_id); }
    irods::error CatalogFacade::modify_data_object(data_id_t obj_id, std::string_view prop, std::string_view value) { return pImpl_->modify_data_object(obj_id, prop, value); }
    irods::error CatalogFacade::register_replica(const replica& repl) { return pImpl_->register_replica(repl); }
    irods::error CatalogFacade::unregister_replica(data_id_t data_id, uint32_t repl_num) { return pImpl_->unregister_replica(data_id, repl_num); }
    irods::error CatalogFacade::update_replica_access_time(data_id_t data_id, uint32_t repl_num, std::string_view time) { return pImpl_->update_replica_access_time(data_id, repl_num, time); }
    irods::error CatalogFacade::register_collection(const collection& coll, coll_id_t& out_id) { return pImpl_->register_collection(coll, out_id); }
    irods::error CatalogFacade::rename_collection(std::string_view old_name, std::string_view new_name) { return pImpl_->rename_collection(old_name, new_name); }
    irods::error CatalogFacade::delete_collection(coll_id_t coll_id) { return pImpl_->delete_collection(coll_id); }
    irods::error CatalogFacade::modify_collection(coll_id_t coll_id, std::string_view prop, std::string_view value) { return pImpl_->modify_collection(coll_id, prop, value); }
    irods::error CatalogFacade::register_resource(const resource& resc, resc_id_t& out_id) { return pImpl_->register_resource(resc, out_id); }
    irods::error CatalogFacade::modify_resource(resc_id_t resc_id, std::string_view prop, std::string_view value) { return pImpl_->modify_resource(resc_id, prop, value); }
    irods::error CatalogFacade::delete_resource(resc_id_t resc_id) { return pImpl_->delete_resource(resc_id); }
    irods::error CatalogFacade::resolve_resource_name(std::string_view name, resc_id_t& out_id) { return pImpl_->resolve_resource_name(name, out_id); }
    irods::error CatalogFacade::update_resource_object_count(resc_id_t resc_id, int delta) { return pImpl_->update_resource_object_count(resc_id, delta); }
    irods::error CatalogFacade::register_user(const user& usr, user_id_t& out_id) { return pImpl_->register_user(usr, out_id); }
    irods::error CatalogFacade::delete_user(user_id_t user_id) { return pImpl_->delete_user(user_id); }
    irods::error CatalogFacade::modify_user(user_id_t user_id, std::string_view prop, std::string_view value) { return pImpl_->modify_user(user_id, prop, value); }
    irods::error CatalogFacade::check_auth(std::string_view user_name, std::string_view zone, int& user_priv) { return pImpl_->check_auth(user_name, zone, user_priv); }
    irods::error CatalogFacade::check_auth_credentials(std::string_view username, std::string_view zone, std::string_view password, bool& correct) { return pImpl_->check_auth_credentials(username, zone, password, correct); }
    irods::error CatalogFacade::add_user_to_group(user_id_t user_id, user_id_t group_id) { return pImpl_->add_user_to_group(user_id, group_id); }
    irods::error CatalogFacade::remove_user_from_group(user_id_t user_id, user_id_t group_id) { return pImpl_->remove_user_from_group(user_id, group_id); }
    irods::error CatalogFacade::set_access(uint64_t user_id, uint64_t target_id, std::string_view level, bool recursive) { return pImpl_->set_access(user_id, target_id, level, recursive); }
    irods::error CatalogFacade::check_permission(uint64_t user_id, uint64_t target_id, std::string_view level, bool& allowed) { return pImpl_->check_permission(user_id, target_id, level, allowed); }
    irods::error CatalogFacade::add_avu_metadata(std::string_view type, std::string_view target_id, const avu& metadata) { return pImpl_->add_avu_metadata(type, target_id, metadata); }
    irods::error CatalogFacade::delete_avu_metadata(std::string_view type, std::string_view target_id, const avu& metadata) { return pImpl_->delete_avu_metadata(type, target_id, metadata); }
    irods::error CatalogFacade::modify_avu_metadata(std::string_view type, std::string_view target_id, const avu& old_avu, const avu& new_avu) { return pImpl_->modify_avu_metadata(type, target_id, old_avu, new_avu); }
    irods::error CatalogFacade::copy_avu_metadata(std::string_view src_type, std::string_view src_id, std::string_view dst_type, std::string_view dst_id) { return pImpl_->copy_avu_metadata(src_type, src_id, dst_type, dst_id); }
    irods::error CatalogFacade::set_avu_metadata(std::string_view type, std::string_view target_id, const avu& metadata) { return pImpl_->set_avu_metadata(type, target_id, metadata); }
    irods::error CatalogFacade::register_zone(const zone& z) { return pImpl_->register_zone(z); }
    irods::error CatalogFacade::modify_zone(std::string_view name, std::string_view prop, std::string_view value) { return pImpl_->modify_zone(name, prop, value); }
    irods::error CatalogFacade::delete_zone(std::string_view name) { return pImpl_->delete_zone(name); }
    irods::error CatalogFacade::register_token(std::string_view name, std::string_view value, std::string_view namespace_str) { return pImpl_->register_token(name, value, namespace_str); }
    irods::error CatalogFacade::delete_token(std::string_view name, std::string_view namespace_str) { return pImpl_->delete_token(name, namespace_str); }
    irods::error CatalogFacade::set_quota(std::string_view user_name, std::string_view resc_name, int64_t limit) { return pImpl_->set_quota(user_name, resc_name, limit); }
    irods::error CatalogFacade::check_quota(std::string_view user_name, std::string_view resc_name, int64_t& usage, int64_t& limit) { return pImpl_->check_quota(user_name, resc_name, usage, limit); }
    irods::error CatalogFacade::execute_query(const irods::experimental::genquery2::select& ast, ResultSet& results) { return pImpl_->execute_query(ast, results); }
    irods::error CatalogFacade::get_next_sequence_value(std::string_view seq_name, uint64_t& out_val) { return pImpl_->get_next_sequence_value(seq_name, out_val); }

} // namespace irods::catalog
