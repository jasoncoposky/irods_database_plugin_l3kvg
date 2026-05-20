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
#include <nlohmann/json.hpp>

#ifdef IRODS_SERVER
#include "irods/rodsLog.h"
#define CAT_LOG(level, ...) rodsLog(level, __VA_ARGS__)
#else
#define CAT_LOG(level, ...) 
#endif

namespace irods::catalog {

    class CatalogImpl {
    public:
        CatalogImpl() : client_(std::make_unique<l3kvg::RemoteL3KVClient>()) {}

        irods::error init(const Config& cfg) {
            try {
                auto pool = std::make_shared<l3kvg::ThreadPool>(4);
                client_->set_thread_pool(pool);

                local_cluster_id_ = SnowflakeID::calculate_cluster_id("tempZone"); 
                client_->add_peer(cfg.node_id, cfg.zmq_endpoint);
                client_->add_peer(local_cluster_id_, cfg.zmq_endpoint);
                for (const auto& fed : cfg.federation) { client_->add_peer(fed.id, fed.endpoint); }
                return SUCCESS();
            } catch (const std::exception& e) { return ERROR(-1, e.what()); }
        }

        irods::error bootstrap_federation(const std::vector<FederatedZone>& peers) {
             for (const auto& peer : peers) {
                 snowflake_id_t zid = (static_cast<uint64_t>(peer.id) << 48) | (XXH3_64bits(peer.name.data(), peer.name.size()) & SnowflakeID::LOCAL_HASH_MASK);
                 lite3cpp::Buffer buf; buf.init_object(); buf.set_str(0, "n", peer.name); buf.set_str(0, "t", "remote");
                 client_->put_node_async(local_cluster_id_, zid, buf.move_to_string());
                 add_index(EntityType::Zone, "name", peer.name, zid);
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

        snowflake_id_t resolve_id_from_index(EntityType type, std::string_view attr, std::string_view value) {
            std::string combined = std::to_string(static_cast<int>(type)) + ":" + std::string(attr) + ":" + std::string(value);
            snowflake_id_t idx_id = SnowflakeID::create(local_cluster_id_, combined);
            std::cerr << "[L3KVG] Resolving Index [" << combined << "] ID [" << std::hex << idx_id << "]" << std::endl;
            auto fut = client_->get_node_payload_async(local_cluster_id_, idx_id);
            std::string payload = fut.get();
            if (payload.empty()) {
                std::cerr << "[L3KVG] Index Payload EMPTY for [" << combined << "]" << std::endl;
                return 0;
            }
            try {
                std::vector<uint8_t> vec(payload.begin(), payload.end());
                lite3cpp::Buffer buf(std::move(vec));
                uint64_t id = static_cast<uint64_t>(buf.get_i64(0, "id"));
                std::cerr << "[L3KVG] Resolved [" << combined << "] to [" << std::hex << id << "]" << std::endl;
                return id;
            } catch (const std::exception& e) { 
                std::cerr << "[L3KVG] BSON Parse FAILED for [" << combined << "]: " << e.what() << std::endl;
                return 0; 
            }
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

        // --- Data Object Operations ---
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
            
            // 1. Fetch and delete all replicas
            auto replicas = client_->get_neighbors_async(local_cluster_id_, sid, "HAS_REPLICA", 0.0).get();
            for (auto rid : replicas) {
                client_->del_node_async(local_cluster_id_, rid);
            }

            // 2. Delete data object node and path index
            std::string payload = client_->get_node_payload_async(local_cluster_id_, sid).get();
            if (!payload.empty()) {
                try {
                    lite3cpp::Buffer buf(std::vector<uint8_t>(payload.begin(), payload.end()));
                    std::string path(buf.get_str(0, "n"));
                    std::string combined = std::to_string(static_cast<int>(EntityType::DataObject)) + ":path:" + path;
                    snowflake_id_t idx_id = SnowflakeID::create(local_cluster_id_, combined);
                    client_->del_node_async(local_cluster_id_, idx_id);
                } catch (...) {}
            }
            client_->del_node_async(local_cluster_id_, sid);
            return SUCCESS(); 
        }
        irods::error rename_data_object(data_id_t obj_id, std::string_view new_name) { 
            snowflake_id_t sid = make_id(EntityType::DataObject, obj_id);
            
            // Update node 'n' property
            std::string payload = client_->get_node_payload_async(local_cluster_id_, sid).get();
            if (!payload.empty()) {
                 lite3cpp::Buffer buf(std::vector<uint8_t>(payload.begin(), payload.end()));
                 buf.set_str(0, "n", std::string(new_name));
                 client_->put_node_async(local_cluster_id_, sid, buf.move_to_string());
            }

            add_index(EntityType::DataObject, "path", new_name, sid);
            return SUCCESS(); 
        }
        irods::error move_data_object(data_id_t obj_id, coll_id_t target_coll_id) { 
            snowflake_id_t sid = make_id(EntityType::DataObject, obj_id);
            snowflake_id_t cid = make_id(EntityType::Collection, target_coll_id);
            
            // Note: In a graph we just add the new edge. 
            // In a real system we should remove the old CONTAINS edge if it exists.
            add_edge(cid, "CONTAINS", 1.0, sid);
            return SUCCESS(); 
        }
        irods::error modify_data_object(data_id_t obj_id, std::string_view prop, std::string_view value) { 
            snowflake_id_t sid = make_id(EntityType::DataObject, obj_id);
            std::string payload = client_->get_node_payload_async(local_cluster_id_, sid).get();
            if (!payload.empty()) {
                 lite3cpp::Buffer buf(std::vector<uint8_t>(payload.begin(), payload.end()));
                 std::string k = (prop == "dataSize") ? "s" : std::string(prop);
                 if (k == "s") buf.set_i64(0, "s", std::stoll(std::string(value)));
                 else buf.set_str(0, k, std::string(value));
                 client_->put_node_async(local_cluster_id_, sid, buf.move_to_string());
            }
            return SUCCESS(); 
        }

        // --- Replica Operations ---
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
        irods::error unregister_replica(data_id_t data_id, uint32_t repl_num) { 
            std::string local_uuid = std::to_string(data_id) + ":" + std::to_string(repl_num);
            snowflake_id_t rid = SnowflakeID::create(local_cluster_id_, local_uuid);
            client_->del_node_async(local_cluster_id_, rid);
            return SUCCESS(); 
        }
        irods::error update_replica_access_time(data_id_t data_id, uint32_t repl_num, std::string_view time) { 
            std::string local_uuid = std::to_string(data_id) + ":" + std::to_string(repl_num);
            snowflake_id_t rid = SnowflakeID::create(local_cluster_id_, local_uuid);
            std::string payload = client_->get_node_payload_async(local_cluster_id_, rid).get();
            if (!payload.empty()) {
                 lite3cpp::Buffer buf(std::vector<uint8_t>(payload.begin(), payload.end()));
                 buf.set_str(0, "mt", std::string(time));
                 client_->put_node_async(local_cluster_id_, rid, buf.move_to_string());
            }
            return SUCCESS(); 
        }

        // --- Collections ---
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
        irods::error rename_collection(std::string_view old_name, std::string_view new_name) { 
            snowflake_id_t sid = resolve_id_from_index(EntityType::Collection, "path", old_name);
            if (!sid) return ERROR(-1, "Collection not found");
            
            // Update node 'n' property
            std::string payload = client_->get_node_payload_async(local_cluster_id_, sid).get();
            if (!payload.empty()) {
                 lite3cpp::Buffer buf(std::vector<uint8_t>(payload.begin(), payload.end()));
                 buf.set_str(0, "n", std::string(new_name));
                 client_->put_node_async(local_cluster_id_, sid, buf.move_to_string());
            }

            add_index(EntityType::Collection, "path", new_name, sid);
            return SUCCESS(); 
        }
        irods::error delete_collection(coll_id_t coll_id) { 
            snowflake_id_t sid = make_id(EntityType::Collection, coll_id);
            
            // Delete path index
            std::string payload = client_->get_node_payload_async(local_cluster_id_, sid).get();
            if (!payload.empty()) {
                try {
                    lite3cpp::Buffer buf(std::vector<uint8_t>(payload.begin(), payload.end()));
                    std::string path(buf.get_str(0, "n"));
                    std::string combined = std::to_string(static_cast<int>(EntityType::Collection)) + ":path:" + path;
                    snowflake_id_t idx_id = SnowflakeID::create(local_cluster_id_, combined);
                    client_->del_node_async(local_cluster_id_, idx_id);
                } catch (...) {}
            }
            client_->del_node_async(local_cluster_id_, sid);
            return SUCCESS(); 
        }
        irods::error modify_collection(coll_id_t coll_id, std::string_view prop, std::string_view value) { 
            snowflake_id_t sid = make_id(EntityType::Collection, coll_id);
            std::string payload = client_->get_node_payload_async(local_cluster_id_, sid).get();
            if (!payload.empty()) {
                 lite3cpp::Buffer buf(std::vector<uint8_t>(payload.begin(), payload.end()));
                 buf.set_str(0, std::string(prop), std::string(value));
                 client_->put_node_async(local_cluster_id_, sid, buf.move_to_string());
            }
            return SUCCESS(); 
        }

        // --- Resources ---
        irods::error register_resource(const resource& resc, resc_id_t& out_id) {
            snowflake_id_t sid = make_id(EntityType::Resource, resc.id);
            lite3cpp::Buffer buf; buf.init_object(); buf.set_str(0, "n", resc.name); buf.set_str(0, "t", resc.type); buf.set_i64(0, "s", static_cast<int64_t>(resc.status));
            client_->put_node_async(local_cluster_id_, sid, buf.move_to_string());
            add_index(EntityType::Resource, "name", resc.name, sid);
            snowflake_id_t zid = make_id(EntityType::Zone, 1);
            add_edge(zid, "HAS_RESC", 1.0, sid);
            out_id = resc.id; return SUCCESS();
        }
        irods::error modify_resource(resc_id_t resc_id, std::string_view prop, std::string_view value) { 
            snowflake_id_t sid = make_id(EntityType::Resource, resc_id);
            std::string payload = client_->get_node_payload_async(local_cluster_id_, sid).get();
            if (!payload.empty()) {
                 lite3cpp::Buffer buf(std::vector<uint8_t>(payload.begin(), payload.end()));
                 buf.set_str(0, std::string(prop), std::string(value));
                 client_->put_node_async(local_cluster_id_, sid, buf.move_to_string());
            }
            return SUCCESS(); 
        }
        irods::error delete_resource(resc_id_t resc_id) { 
            snowflake_id_t sid = make_id(EntityType::Resource, resc_id);
            // Delete name index
            std::string payload = client_->get_node_payload_async(local_cluster_id_, sid).get();
            if (!payload.empty()) {
                try {
                    lite3cpp::Buffer buf(std::vector<uint8_t>(payload.begin(), payload.end()));
                    std::string name(buf.get_str(0, "n"));
                    std::string combined = std::to_string(static_cast<int>(EntityType::Resource)) + ":name:" + name;
                    snowflake_id_t idx_id = SnowflakeID::create(local_cluster_id_, combined);
                    client_->del_node_async(local_cluster_id_, idx_id);
                } catch (...) {}
            }
            client_->del_node_async(local_cluster_id_, sid);
            return SUCCESS(); 
        }
        irods::error resolve_resource_name(std::string_view name, resc_id_t& out_id) { 
            snowflake_id_t sid = resolve_id_from_index(EntityType::Resource, "name", name);
            if (!sid) return ERROR(-1, "Resource not found");
            // Resolve Snowflake ID back to internal ID if possible, or just use Snowflake as internal
            out_id = SnowflakeID::get_local_hash(sid);
            return SUCCESS(); 
        }
        irods::error get_hierarchy_for_resource(std::string_view name, std::string& out_hier) {
            snowflake_id_t sid = resolve_id_from_index(EntityType::Resource, "name", name);
            if (!sid) return ERROR(-1, "Resource not found");

            std::vector<std::string> parts = {std::string(name)};
            snowflake_id_t current = sid;
            
            // Traverse UP using in-neighbors of HAS_CHILD
            while (true) {
                auto parents = client_->get_in_neighbors_async(local_cluster_id_, current, "HAS_CHILD").get();
                if (parents.empty()) break;
                current = parents[0];
                std::string p_payload = client_->get_node_payload_async(local_cluster_id_, current).get();
                if (!p_payload.empty()) {
                    try {
                        lite3cpp::Buffer buf(std::vector<uint8_t>(p_payload.begin(), p_payload.end()));
                        parts.insert(parts.begin(), std::string(buf.get_str(0, "n")));
                    } catch (...) { break; }
                } else break;
            }

            out_hier = "";
            for (size_t i = 0; i < parts.size(); ++i) {
                out_hier += parts[i];
                if (i < parts.size() - 1) out_hier += ";";
            }
            return SUCCESS();
        }
        irods::error add_child_resource(std::string_view parent_name, std::string_view child_name, std::string_view context) { 
            snowflake_id_t pid = resolve_id_from_index(EntityType::Resource, "name", parent_name);
            snowflake_id_t cid = resolve_id_from_index(EntityType::Resource, "name", child_name);
            if (!pid || !cid) return ERROR(-1, "Parent or child resource not found");
            
            add_edge(pid, "HAS_CHILD", 1.0, cid);
            // Optionally store context on the edge or child node.
            return SUCCESS(); 
        }
        irods::error remove_child_resource(std::string_view parent_name, std::string_view child_name) { 
            snowflake_id_t pid = resolve_id_from_index(EntityType::Resource, "name", parent_name);
            snowflake_id_t cid = resolve_id_from_index(EntityType::Resource, "name", child_name);
            if (!pid || !cid) return ERROR(-1, "Parent or child resource not found");
            
            std::string edge_key = std::string(l3kvg::KeyBuilder::edge_out_key(pid, "HAS_CHILD", 1.0, cid));
            client_->del_edge_async(local_cluster_id_, edge_key);
            return SUCCESS(); 
        }
        irods::error update_resource_object_count(resc_id_t resc_id, int delta) { 
            snowflake_id_t sid = make_id(EntityType::Resource, resc_id);
            std::string payload = client_->get_node_payload_async(local_cluster_id_, sid).get();
            if (!payload.empty()) {
                 lite3cpp::Buffer buf(std::vector<uint8_t>(payload.begin(), payload.end()));
                 int64_t count = 0;
                 if (buf.get_type(0, "c") != lite3cpp::Type::Invalid) {
                     count = buf.get_i64(0, "c");
                 }
                 buf.set_i64(0, "c", count + delta);
                 client_->put_node_async(local_cluster_id_, sid, buf.move_to_string());
            }
            return SUCCESS(); 
        }

        // --- Identity ---
        irods::error register_user(const user& usr, user_id_t& out_id) {
            snowflake_id_t sid = make_id(EntityType::User, usr.id);
            lite3cpp::Buffer buf; buf.init_object(); buf.set_str(0, "n", usr.name); buf.set_i64(0, "p", (usr.type == "rodsadmin" ? 5 : 1));
            client_->put_node_async(local_cluster_id_, sid, buf.move_to_string());
            add_index(EntityType::User, "name", usr.name, sid);
            snowflake_id_t zid = make_id(EntityType::Zone, 1);
            add_edge(zid, "HAS_USER", 1.0, sid);
            out_id = usr.id; return SUCCESS();
        }
        irods::error delete_user(std::string_view user_name) { 
            snowflake_id_t uid = resolve_id_from_index(EntityType::User, "name", user_name);
            if (uid) {
                client_->del_node_async(local_cluster_id_, uid);
                std::string combined = std::to_string(static_cast<int>(EntityType::User)) + ":name:" + std::string(user_name);
                snowflake_id_t idx_id = SnowflakeID::create(local_cluster_id_, combined);
                client_->del_node_async(local_cluster_id_, idx_id);
            }
            return SUCCESS(); 
        }
        irods::error modify_user(std::string_view user_name, std::string_view prop, std::string_view value) { 
            snowflake_id_t uid = resolve_id_from_index(EntityType::User, "name", user_name);
            if (!uid) return ERROR(-1, "User not found");
            
            std::string payload = client_->get_node_payload_async(local_cluster_id_, uid).get();
            if (!payload.empty()) {
                 lite3cpp::Buffer buf(std::vector<uint8_t>(payload.begin(), payload.end()));
                 if (prop == "type") {
                     buf.set_i64(0, "p", (value == "rodsadmin" ? 5 : 1));
                 } else if (prop == "password") {
                     buf.set_str(0, "pw", value);
                 }
                 client_->put_node_async(local_cluster_id_, uid, buf.move_to_string());
            }
            return SUCCESS(); 
        }
        irods::error check_auth(std::string_view user_name, std::string_view zone, int& user_priv) {
            snowflake_id_t uid = resolve_id_from_index(EntityType::User, "name", user_name);
            if (!uid) return ERROR(-1, "User not found");
            auto fut = client_->get_node_payload_async(local_cluster_id_, uid);
            std::string payload = fut.get();
            if (payload.empty()) return ERROR(-1, "User node missing");
            try {
                lite3cpp::Buffer buf(std::vector<uint8_t>(payload.begin(), payload.end()));
                user_priv = static_cast<int>(buf.get_i64(0, "p"));
                return SUCCESS();
            } catch (...) { return ERROR(-1, "Failed to parse priv level"); }
        }
        irods::error check_auth_credentials(std::string_view username, std::string_view zone, std::string_view password, bool& correct) {
            correct = false;
            snowflake_id_t uid = resolve_id_from_index(EntityType::User, "name", username);
            if (!uid) return SUCCESS();

            std::string payload = client_->get_node_payload_async(local_cluster_id_, uid).get();
            if (!payload.empty()) {
                try {
                    lite3cpp::Buffer buf(std::vector<uint8_t>(payload.begin(), payload.end()));
                    if (buf.get_type(0, "pw") != lite3cpp::Type::Invalid) {
                        std::string stored_pw(buf.get_str(0, "pw"));
                        correct = (stored_pw == password);
                    } else {
                        correct = true; // Default allow if no PW set yet
                    }
                } catch (...) {}
            }
            return SUCCESS();
        }
        irods::error add_user_to_group(std::string_view user_name, std::string_view zone, std::string_view group_name) { 
            snowflake_id_t uid = resolve_id_from_index(EntityType::User, "name", user_name);
            snowflake_id_t gid = resolve_id_from_index(EntityType::User, "name", group_name);
            if (!uid || !gid) return ERROR(-1, "User or group not found");
            add_edge(uid, "MEMBER_OF", 1.0, gid);
            return SUCCESS(); 
        }
        irods::error remove_user_from_group(std::string_view user_name, std::string_view zone, std::string_view group_name) { 
            snowflake_id_t uid = resolve_id_from_index(EntityType::User, "name", user_name);
            snowflake_id_t gid = resolve_id_from_index(EntityType::User, "name", group_name);
            if (!uid || !gid) return ERROR(-1, "User or group not found");
            
            std::string key = std::string(l3kvg::KeyBuilder::edge_out_key(uid, "MEMBER_OF", 1.0, gid));
            client_->del_edge_async(local_cluster_id_, key);
            return SUCCESS(); 
        }

        // --- ACLs ---
        irods::error set_access(std::string_view user_name, std::string_view zone, std::string_view target_path, std::string_view level, bool recursive) { 
            snowflake_id_t uid = resolve_id_from_index(EntityType::User, "name", user_name);
            if (!uid) return ERROR(-1, "User not found");
            
            // Resolve target path (could be data or collection)
            snowflake_id_t tid = resolve_id_from_index(EntityType::DataObject, "path", target_path);
            if (!tid) tid = resolve_id_from_index(EntityType::Collection, "path", target_path);
            if (!tid) return ERROR(-1, "Target path not found");

            std::string aid_uuid = std::to_string(uid) + ":" + std::to_string(tid);
            snowflake_id_t aid = SnowflakeID::create(local_cluster_id_, aid_uuid);
            
            lite3cpp::Buffer buf; buf.init_object(); buf.set_str(0, "l", std::string(level));
            client_->put_node_async(local_cluster_id_, aid, buf.move_to_string());
            
            add_edge(uid, "HAS_ACCESS", 1.0, aid);
            add_edge(aid, "FOR_OBJECT", 1.0, tid);
            return SUCCESS(); 
        }
        irods::error check_permission(uint64_t user_id, uint64_t target_id, std::string_view level, bool& allowed) { 
            allowed = false;
            snowflake_id_t uid = make_id(EntityType::User, user_id);
            
            // Gather all principals (user + groups)
            std::vector<snowflake_id_t> principals = {uid};
            auto groups = client_->get_neighbors_async(local_cluster_id_, uid, "MEMBER_OF", 0.0).get();
            principals.insert(principals.end(), groups.begin(), groups.end());

            for (auto pid : principals) {
                auto access_nodes = client_->get_neighbors_async(local_cluster_id_, pid, "HAS_ACCESS", 0.0).get();
                for (auto aid : access_nodes) {
                    auto target_nodes = client_->get_neighbors_async(local_cluster_id_, aid, "FOR_OBJECT", 0.0).get();
                    for (auto tid : target_nodes) {
                        // Check if tid matches target_id (we check both data and collection variants)
                        if (tid == make_id(EntityType::DataObject, target_id) || tid == make_id(EntityType::Collection, target_id)) {
                             // Fetch access level
                             std::string payload = client_->get_node_payload_async(local_cluster_id_, aid).get();
                             if (!payload.empty()) {
                                 try {
                                     lite3cpp::Buffer buf(std::vector<uint8_t>(payload.begin(), payload.end()));
                                     std::string actual_level(buf.get_str(0, "l"));
                                     // Basic level check (should be more robust in real system)
                                     if (actual_level == level || actual_level == "own" || (level == "read" && actual_level == "write")) {
                                         allowed = true;
                                         return SUCCESS();
                                     }
                                 } catch (...) {}
                             }
                        }
                    }
                }
            }
            return SUCCESS(); 
        }
        irods::error check_permission_to_modify_data_object(uint64_t user_id, uint64_t target_id, bool& allowed) {
            return check_permission(user_id, target_id, "write", allowed);
        }

        irods::error get_next_sequence_value(std::string_view seq_name, uint64_t& out_val) { 
            // Use a deterministic ID for the sequence node
            snowflake_id_t sid = SnowflakeID::create(local_cluster_id_, "seq:" + std::string(seq_name));
            
            std::string payload = client_->get_node_payload_async(local_cluster_id_, sid).get();
            uint64_t current = 1000; // Start high for safety
            if (!payload.empty()) {
                try {
                    lite3cpp::Buffer buf(std::vector<uint8_t>(payload.begin(), payload.end()));
                    current = static_cast<uint64_t>(buf.get_i64(0, "v"));
                } catch (...) {}
            }
            
            out_val = current + 1;
            lite3cpp::Buffer buf; buf.init_object(); buf.set_i64(0, "v", static_cast<int64_t>(out_val));
            client_->put_node_async(local_cluster_id_, sid, buf.move_to_string()).get();
            
            return SUCCESS(); 
        }

        // --- Metadata ---
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
        irods::error delete_avu_metadata(std::string_view type, std::string_view target_id, const avu& metadata) { 
            std::string local_uuid = metadata.attribute + ":" + metadata.value + ":" + metadata.units;
            snowflake_id_t aid = SnowflakeID::create(local_cluster_id_, local_uuid);
            uint64_t tid_num = std::stoull(std::string(target_id));
            EntityType et = (type == "DataObject" || type == "data") ? EntityType::DataObject : EntityType::Collection;
            snowflake_id_t target_sid = make_id(et, tid_num);

            std::string edge_key = std::string(l3kvg::KeyBuilder::edge_out_key(target_sid, "ANNOTATED_WITH", 1.0, aid));
            client_->del_edge_async(local_cluster_id_, edge_key);
            // We don't delete the metadata node itself because it might be shared (future optimization: ref counting)
            return SUCCESS(); 
        }
        irods::error modify_avu_metadata(std::string_view type, std::string_view target_id, const avu& old_avu, const avu& new_avu) { 
            delete_avu_metadata(type, target_id, old_avu);
            add_avu_metadata(type, target_id, new_avu);
            return SUCCESS(); 
        }
        irods::error copy_avu_metadata(std::string_view src_type, std::string_view src_id, std::string_view dst_type, std::string_view dst_id) { 
            uint64_t s_tid_num = std::stoull(std::string(src_id));
            EntityType s_et = (src_type == "DataObject" || src_type == "data") ? EntityType::DataObject : EntityType::Collection;
            snowflake_id_t src_sid = make_id(s_et, s_tid_num);

            uint64_t d_tid_num = std::stoull(std::string(dst_id));
            EntityType d_et = (dst_type == "DataObject" || dst_type == "data") ? EntityType::DataObject : EntityType::Collection;
            snowflake_id_t dst_sid = make_id(d_et, d_tid_num);

            // Fetch all AVU nodes associated with src
            auto avu_nodes = client_->get_neighbors_async(local_cluster_id_, src_sid, "ANNOTATED_WITH", 0.0).get();
            for (auto aid : avu_nodes) {
                add_edge(dst_sid, "ANNOTATED_WITH", 1.0, aid);
            }
            return SUCCESS(); 
        }
        irods::error set_avu_metadata(std::string_view type, std::string_view target_id, const avu& metadata) { return add_avu_metadata(type, target_id, metadata); }

        // --- Zones ---
        irods::error register_zone(const zone& z) {
            snowflake_id_t zid = make_id(EntityType::Zone, 1);
            lite3cpp::Buffer buf; buf.init_object(); buf.set_str(0, "n", z.name); buf.set_str(0, "t", z.type); buf.set_str(0, "c", z.connection); buf.set_str(0, "m", z.comment);
            client_->put_node_async(local_cluster_id_, zid, buf.move_to_string());
            add_index(EntityType::Zone, "name", z.name, zid);
            return SUCCESS();
        }
        irods::error modify_zone(std::string_view name, std::string_view prop, std::string_view value) { 
            snowflake_id_t zid = resolve_id_from_index(EntityType::Zone, "name", name);
            if (!zid) return ERROR(-1, "Zone not found");
            
            // In a graph we'd patch the node
            // For now, we don't have patch_str_async in RemoteL3KVClient, so we do full put
            std::string payload = client_->get_node_payload_async(local_cluster_id_, zid).get();
            if (!payload.empty()) {
                 lite3cpp::Buffer buf(std::vector<uint8_t>(payload.begin(), payload.end()));
                 buf.set_str(0, std::string(prop), std::string(value));
                 client_->put_node_async(local_cluster_id_, zid, buf.move_to_string());
            }
            return SUCCESS(); 
        }
        irods::error delete_zone(std::string_view name) { 
            snowflake_id_t zid = resolve_id_from_index(EntityType::Zone, "name", name);
            if (zid) {
                client_->del_node_async(local_cluster_id_, zid);
                std::string combined = std::to_string(static_cast<int>(EntityType::Zone)) + ":name:" + std::string(name);
                snowflake_id_t idx_id = SnowflakeID::create(local_cluster_id_, combined);
                client_->del_node_async(local_cluster_id_, idx_id);
            }
            return SUCCESS(); 
        }

        // --- Token & Quota ---
        irods::error register_token(std::string_view name, std::string_view value, std::string_view namespace_str) {
            snowflake_id_t sid = SnowflakeID::create(local_cluster_id_, std::string(namespace_str) + ":" + std::string(name));
            lite3cpp::Buffer buf; buf.init_object(); buf.set_str(0, "n", std::string(name)); buf.set_str(0, "v", std::string(value)); buf.set_str(0, "ns", std::string(namespace_str));
            client_->put_node_async(local_cluster_id_, sid, buf.move_to_string());
            return SUCCESS();
        }
        irods::error delete_token(std::string_view name, std::string_view namespace_str) { 
            snowflake_id_t sid = SnowflakeID::create(local_cluster_id_, std::string(namespace_str) + ":" + std::string(name));
            client_->del_node_async(local_cluster_id_, sid);
            return SUCCESS(); 
        }
        irods::error set_quota(std::string_view user_name, std::string_view resc_name, int64_t limit) { 
            snowflake_id_t uid = resolve_id_from_index(EntityType::User, "name", user_name);
            if (!uid) return ERROR(-1, "User not found");

            std::string q_uuid = "quota:" + std::string(user_name) + ":" + std::string(resc_name);
            snowflake_id_t qid = SnowflakeID::create(local_cluster_id_, q_uuid);

            lite3cpp::Buffer buf; buf.init_object(); buf.set_i64(0, "limit", limit);
            client_->put_node_async(local_cluster_id_, qid, buf.move_to_string());

            add_edge(uid, "HAS_QUOTA", 1.0, qid);
            return SUCCESS(); 
        }
        irods::error check_quota(std::string_view user_name, std::string_view resc_name, int64_t& usage, int64_t& limit) { 
            usage = 0; limit = -1;
            snowflake_id_t uid = resolve_id_from_index(EntityType::User, "name", user_name);
            if (!uid) return SUCCESS();

            std::string q_uuid = "quota:" + std::string(user_name) + ":" + std::string(resc_name);
            snowflake_id_t qid = SnowflakeID::create(local_cluster_id_, q_uuid);

            std::string payload = client_->get_node_payload_async(local_cluster_id_, qid).get();
            if (!payload.empty()) {
                try {
                    lite3cpp::Buffer buf(std::vector<uint8_t>(payload.begin(), payload.end()));
                    limit = buf.get_i64(0, "limit");
                } catch (...) {}
            }
            return SUCCESS(); 
        }
        irods::error calculate_usage(std::string_view user_name, std::string_view resc_name, int64_t& usage) {
            usage = 0; // TODO: Implement graph-based summation
            return SUCCESS();
        }

        // --- Rule Engine ---
        irods::error register_rule_execution(const rule_exec& re, uint64_t& out_id) {
            snowflake_id_t rid = make_id(EntityType::Rule, re.id);
            lite3cpp::Buffer buf; buf.init_object(); 
            buf.set_str(0, "n", re.name); buf.set_str(0, "e", re.exec_time); buf.set_str(0, "p", re.priority);
            client_->put_node_async(local_cluster_id_, rid, buf.move_to_string());
            snowflake_id_t zid = make_id(EntityType::Zone, 1);
            add_edge(zid, "HAS_RULE", 1.0, rid);
            out_id = re.id; return SUCCESS();
        }
        irods::error delete_rule_execution(uint64_t id) {
            snowflake_id_t rid = make_id(EntityType::Rule, id);
            client_->del_node_async(local_cluster_id_, rid);
            return SUCCESS();
        }

        // --- Specific Query ---
        irods::error register_specific_query(std::string_view alias, std::string_view sql) {
            snowflake_id_t sid = SnowflakeID::create(local_cluster_id_, "sq:" + std::string(alias));
            lite3cpp::Buffer buf; buf.init_object(); buf.set_str(0, "a", std::string(alias)); buf.set_str(0, "q", std::string(sql));
            client_->put_node_async(local_cluster_id_, sid, buf.move_to_string());
            return SUCCESS();
        }
        irods::error delete_specific_query(std::string_view alias) {
            snowflake_id_t sid = SnowflakeID::create(local_cluster_id_, "sq:" + std::string(alias));
            client_->del_node_async(local_cluster_id_, sid);
            return SUCCESS();
        }

        // --- Logical Quota ---
        irods::error set_logical_quota(std::string_view coll_name, int64_t limit) {
            snowflake_id_t cid = resolve_id_from_index(EntityType::Collection, "path", coll_name);
            if (!cid) return ERROR(-1, "Collection not found");

            std::string q_uuid = "lquota:" + std::string(coll_name);
            snowflake_id_t qid = SnowflakeID::create(local_cluster_id_, q_uuid);

            lite3cpp::Buffer buf; buf.init_object(); buf.set_i64(0, "limit", limit);
            client_->put_node_async(local_cluster_id_, qid, buf.move_to_string());

            add_edge(cid, "HAS_LOGICAL_QUOTA", 1.0, qid);
            return SUCCESS();
        }
        irods::error check_logical_quota(std::string_view coll_name, int64_t& usage, int64_t& limit) {
            usage = 0; limit = -1;
            snowflake_id_t cid = resolve_id_from_index(EntityType::Collection, "path", coll_name);
            if (!cid) return SUCCESS();

            std::string q_uuid = "lquota:" + std::string(coll_name);
            snowflake_id_t qid = SnowflakeID::create(local_cluster_id_, q_uuid);

            std::string payload = client_->get_node_payload_async(local_cluster_id_, qid).get();
            if (!payload.empty()) {
                try {
                    lite3cpp::Buffer buf(std::vector<uint8_t>(payload.begin(), payload.end()));
                    limit = buf.get_i64(0, "limit");
                } catch (...) {}
            }
            return SUCCESS();
        }
        irods::error calculate_logical_usage(std::string_view coll_name, int64_t& usage) {
            usage = 0; // TODO: Implement graph-based summation
            return SUCCESS();
        }

        // --- Server Load ---
        irods::error register_server_load(std::string_view host, int load) {
            snowflake_id_t sid = SnowflakeID::create(local_cluster_id_, "load:" + std::string(host));
            lite3cpp::Buffer buf; buf.init_object(); buf.set_str(0, "h", std::string(host)); buf.set_i64(0, "l", load);
            client_->put_node_async(local_cluster_id_, sid, buf.move_to_string());
            snowflake_id_t zid = make_id(EntityType::Zone, 1);
            add_edge(zid, "HAS_LOAD", 1.0, sid);
            return SUCCESS();
        }
        irods::error purge_server_load(std::string_view host) {
            snowflake_id_t sid = SnowflakeID::create(local_cluster_id_, "load:" + std::string(host));
            client_->del_node_async(local_cluster_id_, sid);
            return SUCCESS();
        }

        // --- Grid Config ---
        irods::error set_grid_configuration_value(std::string_view key, std::string_view value) {
            snowflake_id_t sid = SnowflakeID::create(local_cluster_id_, "grid:" + std::string(key));
            lite3cpp::Buffer buf; buf.init_object(); buf.set_str(0, "v", std::string(value));
            client_->put_node_async(local_cluster_id_, sid, buf.move_to_string());
            return SUCCESS();
        }
        irods::error get_grid_configuration_value(std::string_view key, std::string& out_value) {
            snowflake_id_t sid = SnowflakeID::create(local_cluster_id_, "grid:" + std::string(key));
            std::string payload = client_->get_node_payload_async(local_cluster_id_, sid).get();
            if (!payload.empty()) {
                try {
                    lite3cpp::Buffer buf(std::vector<uint8_t>(payload.begin(), payload.end()));
                    out_value = std::string(buf.get_str(0, "v"));
                } catch (...) { return ERROR(-1, "Failed to parse grid config"); }
            }
            return SUCCESS();
        }

        // --- Query ---
        irods::error execute_query(const irods::experimental::genquery2::select& ast, ResultSet& results) {
            compiler::Gq2ToL3kvgCompiler compiler;
            std::string query_json = compiler.compile(ast);
            std::vector<uint64_t> starting_nodes; 
            auto fut = client_->resume_query_async(local_cluster_id_, starting_nodes, query_json);
            results.rows = fut.get();
            return SUCCESS();
        }

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
    irods::error CatalogFacade::get_hierarchy_for_resource(std::string_view name, std::string& out_hier) { return pImpl_->get_hierarchy_for_resource(name, out_hier); }
    irods::error CatalogFacade::update_resource_object_count(resc_id_t resc_id, int delta) { return pImpl_->update_resource_object_count(resc_id, delta); }
    irods::error CatalogFacade::add_child_resource(std::string_view parent_name, std::string_view child_name, std::string_view context) { return pImpl_->add_child_resource(parent_name, child_name, context); }
    irods::error CatalogFacade::remove_child_resource(std::string_view parent_name, std::string_view child_name) { return pImpl_->remove_child_resource(parent_name, child_name); }
    irods::error CatalogFacade::register_user(const user& usr, user_id_t& out_id) { return pImpl_->register_user(usr, out_id); }
    irods::error CatalogFacade::delete_user(std::string_view user_name) { return pImpl_->delete_user(user_name); }
    irods::error CatalogFacade::modify_user(std::string_view user_name, std::string_view prop, std::string_view value) { return pImpl_->modify_user(user_name, prop, value); }
    irods::error CatalogFacade::check_auth(std::string_view user_name, std::string_view zone, int& user_priv) { return pImpl_->check_auth(user_name, zone, user_priv); }
    irods::error CatalogFacade::check_auth_credentials(std::string_view username, std::string_view zone, std::string_view password, bool& correct) { return pImpl_->check_auth_credentials(username, zone, password, correct); }
    irods::error CatalogFacade::add_user_to_group(std::string_view user_name, std::string_view zone, std::string_view group_name) { return pImpl_->add_user_to_group(user_name, zone, group_name); }
    irods::error CatalogFacade::remove_user_from_group(std::string_view user_name, std::string_view zone, std::string_view group_name) { return pImpl_->remove_user_from_group(user_name, zone, group_name); }
    irods::error CatalogFacade::set_access(std::string_view user_name, std::string_view zone, std::string_view target_path, std::string_view level, bool recursive) { return pImpl_->set_access(user_name, zone, target_path, level, recursive); }
    irods::error CatalogFacade::check_permission(uint64_t user_id, uint64_t target_id, std::string_view level, bool& allowed) { return pImpl_->check_permission(user_id, target_id, level, allowed); }
    irods::error CatalogFacade::check_permission_to_modify_data_object(uint64_t user_id, uint64_t target_id, bool& allowed) { return pImpl_->check_permission_to_modify_data_object(user_id, target_id, allowed); }

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
    irods::error CatalogFacade::calculate_usage(std::string_view user_name, std::string_view resc_name, int64_t& usage) { return pImpl_->calculate_usage(user_name, resc_name, usage); }
    irods::error CatalogFacade::set_logical_quota(std::string_view coll_name, int64_t limit) { return pImpl_->set_logical_quota(coll_name, limit); }
    irods::error CatalogFacade::check_logical_quota(std::string_view coll_name, int64_t& usage, int64_t& limit) { return pImpl_->check_logical_quota(coll_name, usage, limit); }
    irods::error CatalogFacade::calculate_logical_usage(std::string_view coll_name, int64_t& usage) { return pImpl_->calculate_logical_usage(coll_name, usage); }

    // Server Operations
    irods::error CatalogFacade::register_server_load(std::string_view host, int load) { return pImpl_->register_server_load(host, load); }
    irods::error CatalogFacade::purge_server_load(std::string_view host) { return pImpl_->purge_server_load(host); }

    // Grid Config Operations
    irods::error CatalogFacade::set_grid_configuration_value(std::string_view key, std::string_view value) { return pImpl_->set_grid_configuration_value(key, value); }
    irods::error CatalogFacade::get_grid_configuration_value(std::string_view key, std::string& out_value) { return pImpl_->get_grid_configuration_value(key, out_value); }

    // Rule Operations
    irods::error CatalogFacade::register_rule_execution(const rule_exec& re, uint64_t& out_id) { return pImpl_->register_rule_execution(re, out_id); }
    irods::error CatalogFacade::delete_rule_execution(uint64_t id) { return pImpl_->delete_rule_execution(id); }

    // Specific Query Operations
    irods::error CatalogFacade::register_specific_query(std::string_view alias, std::string_view sql) { return pImpl_->register_specific_query(alias, sql); }
    irods::error CatalogFacade::delete_specific_query(std::string_view alias) { return pImpl_->delete_specific_query(alias); }

    irods::error CatalogFacade::execute_query(const irods::experimental::genquery2::select& ast, ResultSet& results) { return pImpl_->execute_query(ast, results); }
    irods::error CatalogFacade::get_next_sequence_value(std::string_view seq_name, uint64_t& out_val) { return pImpl_->get_next_sequence_value(seq_name, out_val); }

} // namespace irods::catalog
