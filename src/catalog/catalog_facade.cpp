#include "irods/catalog/catalog_facade.hpp"
#include "irods/catalog/binary_key.hpp"
#include "irods/catalog/federation_resolver.hpp"
#include "L3KVG/RemoteL3KVClient.hpp"
#include "L3KVG/Node.hpp"
#include "L3KVG/KeyBuilder.hpp"
#include "irods/catalog/l3kvg_mapper.hpp"
#include "irods/catalog/catalog_schemas.hpp"
#include "irods/catalog/gq2_compiler.hpp"
#include "irods/filesystem/path.hpp"
#include "irods/rodsErrorTable.h"
#include <iostream>
#include <cstdio>
#include <random>
#include <unordered_set>
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
        CatalogImpl() {}

        irods::error init(const Config& cfg, std::string_view zone_name, const l3kvg::Settings& settings) {
            try {
                local_zone_name_ = std::string(zone_name);
                client_ = std::make_unique<l3kvg::RemoteL3KVClient>(settings);
                
                auto pool = std::make_shared<l3kvg::ThreadPool>(1);
                client_->set_thread_pool(pool);

                local_cluster_id_ = cfg.cluster_id; 
                client_->add_peer(cfg.node_id, cfg.zmq_endpoint);
                client_->add_peer(local_cluster_id_, cfg.zmq_endpoint);
                for (const auto& fed : cfg.federation) { client_->add_peer(fed.id, fed.endpoint); }
                if (!cfg.federation.empty()) {
                    bootstrap_federation(cfg.federation);
                }
                return SUCCESS();
            } catch (const std::exception& e) { return ERROR(-1, e.what()); }
        }

        irods::error bootstrap_federation(const std::vector<FederatedZone>& peers) {
             for (const auto& peer : peers) {
                 snowflake_id_t zid = (static_cast<uint64_t>(peer.id) << 48) | (XXH3_64bits(peer.name.data(), peer.name.size()) & SnowflakeID::LOCAL_HASH_MASK);
                 lite3cpp::Buffer buf; buf.init_object(); buf.set_str(0, "n", peer.name); buf.set_str(0, "t", "remote");
                 client_->put_node_async(local_cluster_id_, zid, buf.move_to_string()).get();
                 add_index(EntityType::Zone, "t", "remote", zid);
             }
             return SUCCESS();
        }

        snowflake_id_t make_id(EntityType type, uint64_t irods_id) {
            std::string local_uuid = std::to_string(static_cast<int>(type)) + ":" + std::to_string(irods_id);
            return SnowflakeID::create(local_cluster_id_, local_uuid);
        }

        std::string safe_get_str(lite3cpp::Buffer& buf, int row, std::string_view key) {
            try {
                auto type = buf.get_type(row, key);
                if (type == lite3cpp::Type::String) return std::string(buf.get_str(row, key));
                if (type == lite3cpp::Type::Int64) return std::to_string(buf.get_i64(row, key));
                if (type == lite3cpp::Type::Float64) return std::to_string(buf.get_f64(row, key));
                return "";
            } catch (...) { return ""; }
        }

        std::string get_idx_key(EntityType type, std::string_view attr, std::string_view value) {
             std::string type_str;
             switch(type) {
                 case EntityType::Zone: type_str = "Zone"; break;
                 case EntityType::User: type_str = "User"; break;
                 case EntityType::Collection: type_str = "Collection"; break;
                 case EntityType::DataObject: type_str = "DataObject"; break;
                 case EntityType::Resource: type_str = "Resource"; break;
                 case EntityType::Replica: type_str = "Replica"; break;
                 case EntityType::Rule: type_str = "Rule"; break;
                 case EntityType::Metadata: type_str = "Metadata"; break;
                 case EntityType::Ticket: type_str = "Ticket"; break;
                 default: type_str = std::to_string(static_cast<int>(type)); break;
             }
             if (attr == "t") return "idx:type:" + std::string(value);
             return "idx:" + type_str + ":" + std::string(attr) + ":" + std::string(value);
        }

        void add_index(EntityType type, std::string_view attr, std::string_view value, snowflake_id_t target_id) {
             if (attr == "t") {
                 // For types, we create an edge from a central type node to the target node
                 snowflake_id_t type_sid = SnowflakeID::create(local_cluster_id_, "idx:type:" + std::string(value));
                 add_edge(type_sid, "HAS_INSTANCE", 1.0, target_id);
                 return;
             }
             std::string idx_key = get_idx_key(type, attr, value);
             char id_hex[17];
             std::snprintf(id_hex, sizeof(id_hex), "%016llx", (unsigned long long)target_id);
             #ifdef IRODS_SERVER
             rodsLog(LOG_NOTICE, "L3_CATALOG: add_index idx_key=[%s] id_hex=[%s]", idx_key.c_str(), id_hex);
             #endif
             client_->put_edge_async(local_cluster_id_, idx_key, id_hex).get();
        }


        void del_index(EntityType type, std::string_view attr, std::string_view value) {
             std::string idx_key = get_idx_key(type, attr, value);
             #ifdef IRODS_SERVER
             rodsLog(LOG_NOTICE, "L3_CATALOG: del_index idx_key=[%s]", idx_key.c_str());
             #endif
             client_->del_edge_async(local_cluster_id_, idx_key).get();
        }

        snowflake_id_t resolve_id_from_index(EntityType type, std::string_view attr, std::string_view value) {
             try {
                 std::string idx_key = get_idx_key(type, attr, value);
                 auto fut = client_->get_raw_key_async(local_cluster_id_, idx_key);
                 std::string payload = fut.get();
                 #ifdef IRODS_SERVER
                 rodsLog(LOG_NOTICE, "L3_CATALOG: resolve_id_from_index idx_key=[%s] payload=[%s]", idx_key.c_str(), payload.c_str());
                 #endif
                 if (payload.empty()) return 0;
                 return std::stoull(payload, nullptr, 16);
             } catch (...) {
                 return 0;
             }
        }

        void add_edge(snowflake_id_t src, std::string_view label, double weight, snowflake_id_t dst) {
            std::string out_key = std::string(l3kvg::KeyBuilder::edge_out_key(src, label, weight, dst));
            client_->put_edge_async(local_cluster_id_, out_key, "{}").get();
            std::string in_key = std::string(l3kvg::KeyBuilder::edge_in_key(dst, label, src));
            client_->put_edge_async(local_cluster_id_, in_key, "{}").get();
        }

        void del_edge(snowflake_id_t src, std::string_view label, double weight, snowflake_id_t dst) {
            std::string out_key = std::string(l3kvg::KeyBuilder::edge_out_key(src, label, weight, dst));
            client_->del_edge_async(local_cluster_id_, out_key).get();
            std::string in_key = std::string(l3kvg::KeyBuilder::edge_in_key(dst, label, src));
            client_->del_edge_async(local_cluster_id_, in_key).get();
        }

        irods::error bootstrap_catalog(std::string_view zone_name, std::string_view admin_name) {
            local_zone_name_ = std::string(zone_name);
            // Initialize sequences
            auto init_seq = [&](std::string_view name, uint64_t start) {
                std::string seq_key = "seq:" + std::string(name);
                lite3cpp::Buffer sbuf; sbuf.init_object(); sbuf.set_i64(0, "v", start);
                client_->put_edge_async(local_cluster_id_, seq_key, sbuf.move_to_string()).get();
            };
            init_seq("R_USER_MAIN", 100); // Start higher to avoid bootstrap collisions
            init_seq("R_COLL_MAIN", 1000);
            init_seq("R_DATA_MAIN", 1000);
            init_seq("R_RESC_MAIN", 40010);
            init_seq("R_ZONE_MAIN", 10);

            char time_buf[32];
            snprintf(time_buf, sizeof(time_buf), "%011lld", (long long)time(nullptr));
            snowflake_id_t zid = make_id(EntityType::Zone, 1);
            lite3cpp::Buffer zbuf; zbuf.init_object(); 
            zbuf.set_str(0, "n", std::string(zone_name)); 
            zbuf.set_str(0, "t", "local");
            zbuf.set_str(0, "c", "");
            zbuf.set_str(0, "m", "");
            zbuf.set_str(0, "ct", time_buf);
            zbuf.set_str(0, "mt", time_buf);
            zbuf.set_i64(0, "id", 1);
            client_->put_node_async(local_cluster_id_, zid, zbuf.move_to_string()).get();
            add_index(EntityType::Zone, "n", zone_name, zid);
            add_index(EntityType::Zone, "t", "local", zid);
            add_index(EntityType::Zone, "id", "1", zid);
            
            snowflake_id_t uid = make_id(EntityType::User, 1);
            lite3cpp::Buffer ubuf; ubuf.init_object(); 
            ubuf.set_str(0, "n", std::string(admin_name)); 
            ubuf.set_str(0, "t", "rodsadmin"); 
            ubuf.set_str(0, "z", std::string(zone_name));
            ubuf.set_i64(0, "p", 5); 
            ubuf.set_str(0, "pw", "rods");
            ubuf.set_i64(0, "id", 1);
            client_->put_node_async(local_cluster_id_, uid, ubuf.move_to_string()).get();
            add_index(EntityType::User, "n", admin_name, uid);
            add_index(EntityType::User, "id", "1", uid);
            add_edge(zid, "HAS_USER", 1.0, uid);

            // Bootstrap default groups
            snowflake_id_t gid_public = make_id(EntityType::User, 2);
            lite3cpp::Buffer gbuf_public; gbuf_public.init_object(); 
            gbuf_public.set_str(0, "n", "public"); 
            gbuf_public.set_str(0, "t", "rodsgroup"); 
            gbuf_public.set_str(0, "z", std::string(zone_name));
            gbuf_public.set_i64(0, "id", 2);
            client_->put_node_async(local_cluster_id_, gid_public, gbuf_public.move_to_string()).get();
            add_index(EntityType::User, "n", "public", gid_public);
            add_index(EntityType::User, "id", "2", gid_public);
            add_edge(zid, "HAS_USER", 1.0, gid_public);

            snowflake_id_t gid_admin = make_id(EntityType::User, 3);
            lite3cpp::Buffer gbuf_admin; gbuf_admin.init_object(); 
            gbuf_admin.set_str(0, "n", "rodsadmin"); 
            gbuf_admin.set_str(0, "t", "rodsgroup"); 
            gbuf_admin.set_str(0, "z", std::string(zone_name));
            gbuf_admin.set_i64(0, "id", 3);
            client_->put_node_async(local_cluster_id_, gid_admin, gbuf_admin.move_to_string()).get();
            add_index(EntityType::User, "n", "rodsadmin", gid_admin);
            add_index(EntityType::User, "id", "3", gid_admin);
            add_edge(zid, "HAS_USER", 1.0, gid_admin);

            // Add rods user to rodsadmin group
            add_edge(uid, "MEMBER_OF", 1.0, gid_admin);

            // Register Standard Collections
            // True Root Collection "/"
            std::string sys_root_coll = "/";
            snowflake_id_t sys_root_cid = make_id(EntityType::Collection, 9999);
            lite3cpp::Buffer cbuf;
            cbuf.init_object(); cbuf.set_str(0, "n", sys_root_coll); cbuf.set_str(0, "pn", "/");
            cbuf.set_str(0, "o", std::string(admin_name)); cbuf.set_str(0, "z", std::string(zone_name));
            cbuf.set_i64(0, "id", 9999);
            cbuf.set_str(0, "ct", "01748200000"); cbuf.set_str(0, "mt", "01748200000");
            client_->put_node_async(local_cluster_id_, sys_root_cid, cbuf.move_to_string()).get();
            add_index(EntityType::Collection, "n", sys_root_coll, sys_root_cid);
            add_index(EntityType::Collection, "id", "9999", sys_root_cid);
            add_edge(zid, "HAS_ROOT_COLL", 1.0, sys_root_cid);
            set_access(admin_name, zone_name, sys_root_coll, "own", false);

            std::string root_coll_name = "/" + std::string(zone_name);
            std::string home_coll_name = root_coll_name + "/home";
            std::string public_coll_name = home_coll_name + "/public";
            
            snowflake_id_t rcid = make_id(EntityType::Collection, 1);
            snowflake_id_t hcid = make_id(EntityType::Collection, 2);
            snowflake_id_t pcid = make_id(EntityType::Collection, 3);
            
            // Zone Root /<zone_name>
            cbuf.init_object(); cbuf.set_str(0, "n", root_coll_name); cbuf.set_str(0, "pn", "/");
            cbuf.set_str(0, "o", std::string(admin_name)); cbuf.set_str(0, "z", std::string(zone_name));
            cbuf.set_i64(0, "id", 1);
            cbuf.set_str(0, "ct", "01748200000"); cbuf.set_str(0, "mt", "01748200000");
            client_->put_node_async(local_cluster_id_, rcid, cbuf.move_to_string()).get();
            add_index(EntityType::Collection, "n", root_coll_name, rcid);
            add_index(EntityType::Collection, "id", "1", rcid);
            add_edge(sys_root_cid, "CONTAINS", 1.0, rcid);
            add_edge(zid, "HAS_ROOT_COLL", 1.0, rcid);
            set_access(admin_name, zone_name, root_coll_name, "own", false);
            
            // home
            cbuf.init_object(); cbuf.set_str(0, "n", home_coll_name); cbuf.set_str(0, "pn", root_coll_name);
            cbuf.set_str(0, "o", std::string(admin_name)); cbuf.set_str(0, "z", std::string(zone_name));
            cbuf.set_i64(0, "id", 2);
            client_->put_node_async(local_cluster_id_, hcid, cbuf.move_to_string()).get();
            add_index(EntityType::Collection, "n", home_coll_name, hcid);
            add_index(EntityType::Collection, "id", "2", hcid);
            add_edge(rcid, "CONTAINS", 1.0, hcid);
            
            // public
            cbuf.init_object(); cbuf.set_str(0, "n", public_coll_name); cbuf.set_str(0, "pn", home_coll_name);
            cbuf.set_str(0, "o", std::string(admin_name)); cbuf.set_str(0, "z", std::string(zone_name));
            cbuf.set_i64(0, "id", 3);
            client_->put_node_async(local_cluster_id_, pcid, cbuf.move_to_string()).get();
            add_index(EntityType::Collection, "n", public_coll_name, pcid);
            add_index(EntityType::Collection, "id", "3", pcid);
            add_edge(hcid, "CONTAINS", 1.0, pcid);

            // trash
            std::string trash_coll_name = root_coll_name + "/trash";
            snowflake_id_t tcid = make_id(EntityType::Collection, 4);
            cbuf.init_object(); cbuf.set_str(0, "n", trash_coll_name); cbuf.set_str(0, "pn", root_coll_name);
            cbuf.set_str(0, "o", std::string(admin_name)); cbuf.set_str(0, "z", std::string(zone_name));
            cbuf.set_i64(0, "id", 4);
            client_->put_node_async(local_cluster_id_, tcid, cbuf.move_to_string()).get();
            add_index(EntityType::Collection, "n", trash_coll_name, tcid);
            add_index(EntityType::Collection, "id", "4", tcid);
            add_edge(rcid, "CONTAINS", 1.0, tcid);

            // trash/home
            std::string trash_home_coll_name = trash_coll_name + "/home";
            snowflake_id_t thcid = make_id(EntityType::Collection, 5);
            cbuf.init_object(); cbuf.set_str(0, "n", trash_home_coll_name); cbuf.set_str(0, "pn", trash_coll_name);
            cbuf.set_str(0, "o", std::string(admin_name)); cbuf.set_str(0, "z", std::string(zone_name));
            cbuf.set_i64(0, "id", 5);
            client_->put_node_async(local_cluster_id_, thcid, cbuf.move_to_string()).get();
            add_index(EntityType::Collection, "n", trash_home_coll_name, thcid);
            add_index(EntityType::Collection, "id", "5", thcid);
            add_edge(tcid, "CONTAINS", 1.0, thcid);

            return SUCCESS();
        }

        // --- Data Object Operations ---
        irods::error register_data_object(const data_object& obj, data_id_t& out_id) {
            std::string full_path = obj.full_path;
            if (full_path.empty() && obj.coll_id != 0) {
                snowflake_id_t csid = make_id(EntityType::Collection, obj.coll_id);
                std::string cpayload = client_->get_node_payload_async(local_cluster_id_, csid).get();
                if (!cpayload.empty()) {
                    try {
                        lite3cpp::Buffer cbuf(std::vector<uint8_t>(cpayload.begin(), cpayload.end()));
                        std::string cname = safe_get_str(cbuf, 0, "n");
                        if (!cname.empty()) {
                            if (cname.back() == '/') {
                                full_path = cname + obj.name;
                            } else {
                                full_path = cname + "/" + obj.name;
                            }
                        }
                    } catch (...) {}
                }
            }

            // Check if collection exists with this path
            if (!full_path.empty()) {
                snowflake_id_t existing_coll = resolve_id_from_index(EntityType::Collection, "n", full_path);
                if (existing_coll) {
                    return ERROR(CAT_NAME_EXISTS_AS_COLLECTION, "Collection already exists with data object name: " + full_path);
                }
            }

            snowflake_id_t sid = make_id(EntityType::DataObject, obj.id);
            #ifdef IRODS_SERVER
            rodsLog(LOG_NOTICE, "L3_CATALOG: Registering DataObject [%s] with ID [%llu] (SID: %016llx) in Coll [%llu]", obj.name.c_str(), (unsigned long long)obj.id, (unsigned long long)sid, (unsigned long long)obj.coll_id);
            #endif
            lite3cpp::Buffer buf; buf.init_object(); 
            buf.set_str(0, "n", obj.name); buf.set_str(0, "o", obj.owner_name); buf.set_i64(0, "s", obj.size); 
            buf.set_str(0, "t", obj.type);
            buf.set_str(0, "entity_type", "data_object");
            buf.set_str(0, "p", full_path);
            buf.set_str(0, "ct", obj.create_ts); buf.set_str(0, "mt", obj.modify_ts);
            buf.set_i64(0, "id", static_cast<int64_t>(obj.id));
            std::string expiry = obj.expiry.empty() ? "00000000000" : obj.expiry;
            buf.set_str(0, "ex", expiry);
            if (!obj.owner_zone.empty()) buf.set_str(0, "z", obj.owner_zone);
            if (!obj.mode.empty()) buf.set_str(0, "mode", obj.mode);
            if (!obj.version.empty()) buf.set_str(0, "v", obj.version);
            if (!obj.comments.empty()) buf.set_str(0, "c", obj.comments);
            if (!obj.status.empty()) buf.set_str(0, "st", obj.status);
            client_->put_node_async(local_cluster_id_, sid, buf.move_to_string()).get();
            add_index(EntityType::DataObject, "n", obj.name, sid);
            add_index(EntityType::DataObject, "id", std::to_string(obj.id), sid);
            if (!full_path.empty()) {
                add_index(EntityType::DataObject, "path", full_path, sid);
            }
            
            snowflake_id_t cid = make_id(EntityType::Collection, obj.coll_id);
            #ifdef IRODS_SERVER
            rodsLog(LOG_NOTICE, "L3_CATALOG: Creating CONTAINS edge: %016llx -- CONTAINS --> %016llx", (unsigned long long)cid, (unsigned long long)sid);
            #endif
            add_edge(cid, "CONTAINS", 1.0, sid);

            snowflake_id_t uid = resolve_id_from_index(EntityType::User, "n", obj.owner_name);
            if (uid) {
                #ifdef IRODS_SERVER
                rodsLog(LOG_NOTICE, "L3_CATALOG: Creating OWNS edge: %016llx -- OWNS --> %016llx", (unsigned long long)uid, (unsigned long long)sid);
                #endif
                add_edge(uid, "OWNS", 1.0, sid);
            }
            
            // Implicitly grant 'own' access to the creator
            // Use full path if available for unique resolution
            auto access_ret = set_access(obj.owner_name, obj.owner_zone, (full_path.empty() ? obj.name : full_path), "own", false);
            if (!access_ret.ok()) {
                #ifdef IRODS_SERVER
                rodsLog(LOG_NOTICE, "L3_CATALOG: register_data_object failed to set owner access for [%s]: %s", obj.name.c_str(), access_ret.result().c_str());
                #endif
            }

            out_id = obj.id; return SUCCESS();
        }
        irods::error delete_data_object(data_id_t id) { 
            snowflake_id_t sid = make_id(EntityType::DataObject, id);
            
            #ifdef IRODS_SERVER
            rodsLog(LOG_NOTICE, "L3_CATALOG: Deleting DataObject %llu (SID: %016llx)", (unsigned long long)id, (unsigned long long)sid);
            #endif

            // 1. Fetch and delete all replicas
            auto replicas = client_->get_neighbors_async(local_cluster_id_, sid, "HAS_REPLICA", 0.0).get();
            for (auto rid : replicas) {
                del_edge(sid, "HAS_REPLICA", 1.0, rid);
                client_->del_node_async(local_cluster_id_, rid).get();
            }

            // 2. Cleanup indices and edges
            std::string payload = client_->get_node_payload_async(local_cluster_id_, sid).get();
            if (!payload.empty()) {
                try {
                    lite3cpp::Buffer buf(std::vector<uint8_t>(payload.begin(), payload.end()));
                    std::string name = safe_get_str(buf, 0, "n");
                    del_index(EntityType::DataObject, "n", name);
                    std::string id_str = safe_get_str(buf, 0, "id");
                    if (!id_str.empty()) {
                        del_index(EntityType::DataObject, "id", id_str);
                    } else if (id != 0) {
                        del_index(EntityType::DataObject, "id", std::to_string(id));
                    }

                    std::string path = safe_get_str(buf, 0, "p");
                    if (!path.empty()) {
                        del_index(EntityType::DataObject, "path", path);
                    }

                    // Delete incoming edges (CONTAINS, OWNS)
                    auto collections = client_->get_in_neighbors_async(local_cluster_id_, sid, "CONTAINS").get();
                    for (auto cid : collections) {
                        del_edge(cid, "CONTAINS", 1.0, sid);
                    }
                    auto owners = client_->get_in_neighbors_async(local_cluster_id_, sid, "OWNS").get();
                    for (auto oid : owners) {
                        del_edge(oid, "OWNS", 1.0, sid);
                    }
                    auto accesses = client_->get_in_neighbors_async(local_cluster_id_, sid, "FOR_OBJECT").get();
                    for (auto aid : accesses) {
                        del_edge(aid, "FOR_OBJECT", 1.0, sid);
                        client_->del_node_async(local_cluster_id_, aid).get();
                    }
                } catch (...) {}
            }
            
            client_->del_node_async(local_cluster_id_, sid).get();
            return SUCCESS(); 
        }
        irods::error rename_data_object(data_id_t obj_id, std::string_view new_name) { 
            snowflake_id_t sid = make_id(EntityType::DataObject, obj_id);
            
            // Update node 'n' property
            std::string payload = client_->get_node_payload_async(local_cluster_id_, sid).get();
            if (!payload.empty()) {
                 lite3cpp::Buffer old_buf(std::vector<uint8_t>(payload.begin(), payload.end()));
                 lite3cpp::Buffer new_buf; new_buf.init_object();
                 
                 std::string old_name = safe_get_str(old_buf, 0, "n");
                 std::string owner = safe_get_str(old_buf, 0, "o");
                 int64_t size = 0;
                 try { size = old_buf.get_i64(0, "s"); } catch(...) {}
                 std::string type = safe_get_str(old_buf, 0, "t");
                 std::string old_path = safe_get_str(old_buf, 0, "p");
                 std::string ct = safe_get_str(old_buf, 0, "ct");
                 char time_buf[50];
                 snprintf(time_buf, sizeof(time_buf), "%011lld", (long long)time(nullptr));
                 std::string mt(time_buf);
                 int64_t id = static_cast<int64_t>(obj_id);
                 try { id = old_buf.get_i64(0, "id"); } catch(...) {}

                 std::string new_path = std::string(new_name);
                 std::string base_name = std::string(new_name);
                 size_t slash = new_path.find_last_of('/');
                 if (slash != std::string::npos) {
                     base_name = new_path.substr(slash + 1);
                     std::string new_parent_path = (slash == 0) ? "/" : new_path.substr(0, slash);
                     std::string old_parent_path;
                     if (!old_path.empty()) {
                         size_t old_slash = old_path.find_last_of('/');
                         old_parent_path = (old_slash == 0) ? "/" : (old_slash != std::string::npos ? old_path.substr(0, old_slash) : "");
                     }
                     if (!old_parent_path.empty() && old_parent_path != new_parent_path) {
                         snowflake_id_t old_psid = resolve_id_from_index(EntityType::Collection, "n", old_parent_path);
                         snowflake_id_t new_psid = resolve_id_from_index(EntityType::Collection, "n", new_parent_path);
                         if (old_psid) del_edge(old_psid, "CONTAINS", 1.0, sid);
                         if (new_psid) add_edge(new_psid, "CONTAINS", 1.0, sid);
                     }
                 } else if (!old_path.empty()) {
                     size_t old_slash = old_path.find_last_of('/');
                     if (old_slash != std::string::npos) {
                         new_path = old_path.substr(0, old_slash + 1) + std::string(new_name);
                     }
                 }

                 old_buf.set_str(0, "n", base_name);
                 old_buf.set_str(0, "p", new_path);
                 old_buf.set_str(0, "mt", mt);

                 client_->put_node_async(local_cluster_id_, sid, old_buf.move_to_string()).get();

                 if (!old_name.empty()) del_index(EntityType::DataObject, "n", old_name);
                 if (!old_path.empty()) del_index(EntityType::DataObject, "path", old_path);
                 add_index(EntityType::DataObject, "n", base_name, sid);
                 add_index(EntityType::DataObject, "path", new_path, sid);
            }
            return SUCCESS(); 
        }
        irods::error move_data_object(data_id_t obj_id, coll_id_t target_coll_id) { 
            snowflake_id_t sid = make_id(EntityType::DataObject, obj_id);
            snowflake_id_t cid = make_id(EntityType::Collection, target_coll_id);
            
            std::string d_payload = client_->get_node_payload_async(local_cluster_id_, sid).get();
            if (d_payload.empty()) return ERROR(CAT_UNKNOWN_FILE, "Data object not found");

            std::string c_payload = client_->get_node_payload_async(local_cluster_id_, cid).get();
            if (c_payload.empty()) return ERROR(CAT_UNKNOWN_COLLECTION, "Target collection not found");

            lite3cpp::Buffer dbuf(std::vector<uint8_t>(d_payload.begin(), d_payload.end()));
            lite3cpp::Buffer cbuf(std::vector<uint8_t>(c_payload.begin(), c_payload.end()));

            std::string data_name = safe_get_str(dbuf, 0, "n");
            std::string old_path = safe_get_str(dbuf, 0, "p");
            std::string target_coll_path = safe_get_str(cbuf, 0, "n");
            std::string new_path = (target_coll_path == "/" ? "/" : target_coll_path + "/") + data_name;

            // Remove old CONTAINS edge(s) from collection(s) to this data object
            auto in_colls = client_->get_in_neighbors_async(local_cluster_id_, sid, "CONTAINS").get();
            for (auto old_cid : in_colls) {
                del_edge(old_cid, "CONTAINS", 1.0, sid);
            }

            // Add new CONTAINS edge
            add_edge(cid, "CONTAINS", 1.0, sid);

            // Update data object path and timestamp
            char time_buf[50];
            snprintf(time_buf, sizeof(time_buf), "%011lld", (long long)time(nullptr));
            dbuf.set_str(0, "p", new_path);
            dbuf.set_str(0, "mt", std::string(time_buf));
            client_->put_node_async(local_cluster_id_, sid, dbuf.move_to_string()).get();

            if (!old_path.empty()) {
                del_index(EntityType::DataObject, "path", old_path);
            }
            add_index(EntityType::DataObject, "path", new_path, sid);

            return SUCCESS(); 
        }
        irods::error modify_data_object(data_id_t obj_id, std::string_view prop, std::string_view value) { 
            snowflake_id_t sid = make_id(EntityType::DataObject, obj_id);
            std::string payload = client_->get_node_payload_async(local_cluster_id_, sid).get();
            if (!payload.empty()) {
                 lite3cpp::Buffer buf(std::vector<uint8_t>(payload.begin(), payload.end()));
                 std::string k = std::string(prop);
                 if (prop == "dataSize" || prop == "data_size" || prop == "DATA_SIZE" || prop == "size" || prop == "s") k = "s";
                 else if (prop == "dataModify" || prop == "modify_ts" || prop == "DATA_MODIFY_TIME" || prop == "mt") k = "mt";
                 else if (prop == "dataCreate" || prop == "create_ts" || prop == "DATA_CREATE_TIME" || prop == "ct") k = "ct";
                 else if (prop == "dataComments" || prop == "r_comment" || prop == "DATA_COMMENTS" || prop == "cm" || prop == "c") k = "c";
                 else if (prop == "dataType" || prop == "data_type_name" || prop == "DATA_TYPE_NAME" || prop == "t") k = "t";
                 else if (prop == "dataOwner" || prop == "data_owner_name" || prop == "DATA_OWNER_NAME" || prop == "o") k = "o";
                 else if (prop == "dataOwnerZone" || prop == "data_owner_zone" || prop == "DATA_OWNER_ZONE" || prop == "z") k = "z";
                 else if (prop == "dataMode" || prop == "data_mode" || prop == "DATA_MODE" || prop == "mode") k = "mode";
                 else if (prop == "dataExpiry" || prop == "data_expiry" || prop == "DATA_EXPIRY" || prop == "DATA_EXPIRY_KW" || prop == "data_expiry_ts" || prop == "ex") k = "ex";

                 if (k == "s") {
                     try { buf.set_i64(0, "s", std::stoll(std::string(value))); } catch (...) {}
                 } else {
                     buf.set_str(0, k, std::string(value));
                 }
                 client_->put_node_async(local_cluster_id_, sid, buf.move_to_string()).get();
            }
            return SUCCESS(); 
        }

        // --- Replica Operations ---
        irods::error register_replica(const replica& repl) {
            std::string local_uuid = std::to_string(repl.data_id) + ":" + std::to_string(repl.replica_number);
            snowflake_id_t rid = SnowflakeID::create(local_cluster_id_, local_uuid);
            #ifdef IRODS_SERVER
            rodsLog(LOG_NOTICE, "L3_CATALOG: Registering Replica [DataID: %llu, Num: %u] (SID: %016llx) at Resc [%llu]", (unsigned long long)repl.data_id, repl.replica_number, (unsigned long long)rid, (unsigned long long)repl.resource_id);
            #endif
            lite3cpp::Buffer buf; buf.init_object(); 
            buf.set_i64(0, "id", repl.data_id);
            buf.set_i64(0, "rn", repl.replica_number); 
            buf.set_str(0, "p", repl.physical_path); 
            buf.set_str(0, "rh", repl.resc_hier); 
            buf.set_str(0, "t", "replica");
            buf.set_str(0, "st", repl.status); 
            buf.set_str(0, "cs", repl.checksum);
            buf.set_i64(0, "rid", repl.resource_id);
            buf.set_str(0, "mt", repl.modify_ts);
            buf.set_i64(0, "s", repl.size);
            client_->put_node_async(local_cluster_id_, rid, buf.move_to_string()).get();
            
            snowflake_id_t data_sid = make_id(EntityType::DataObject, repl.data_id);
            snowflake_id_t resc_sid = make_id(EntityType::Resource, repl.resource_id);
            #ifdef IRODS_SERVER
            rodsLog(LOG_NOTICE, "L3_CATALOG: Creating HAS_REPLICA edge: %016llx -- HAS_REPLICA --> %016llx", (unsigned long long)data_sid, (unsigned long long)rid);
            #endif
            add_edge(data_sid, "HAS_REPLICA", 1.0, rid);
            #ifdef IRODS_SERVER
            rodsLog(LOG_NOTICE, "L3_CATALOG: Creating STAYING_AT edge: %016llx -- STAYING_AT --> %016llx", (unsigned long long)rid, (unsigned long long)resc_sid);
            #endif
            add_edge(rid, "STAYING_AT", 1.0, resc_sid);
            
            return SUCCESS();
        }
        irods::error unregister_replica(data_id_t data_id, uint32_t repl_num) { 
            std::string local_uuid = std::to_string(data_id) + ":" + std::to_string(repl_num);
            snowflake_id_t rid = SnowflakeID::create(local_cluster_id_, local_uuid);
            snowflake_id_t sid = make_id(EntityType::DataObject, data_id);

            #ifdef IRODS_SERVER
            rodsLog(LOG_NOTICE, "L3_CATALOG: Unregistering Replica [DataID: %llu, Num: %u] (SID: %016llx)", (unsigned long long)data_id, repl_num, (unsigned long long)rid);
            #endif

            // Delete Edges first
            del_edge(sid, "HAS_REPLICA", 1.0, rid);

            std::string payload = client_->get_node_payload_async(local_cluster_id_, rid).get();
            if (!payload.empty()) {
                try {
                    lite3cpp::Buffer buf(std::vector<uint8_t>(payload.begin(), payload.end()));
                    uint64_t resc_id = buf.get_i64(0, "rid");
                    if (resc_id != 0) {
                        snowflake_id_t rsid = make_id(EntityType::Resource, resc_id);
                        del_edge(rid, "STAYING_AT", 1.0, rsid);
                    }
                } catch (...) {}
            }

            client_->del_node_async(local_cluster_id_, rid).get();

            // Check if any replicas remain for this DataObject
            auto replicas = client_->get_neighbors_async(local_cluster_id_, sid, "HAS_REPLICA", 0.0).get();
            if (replicas.empty()) {
                #ifdef IRODS_SERVER
                rodsLog(LOG_NOTICE, "L3_CATALOG: Last replica removed, deleting DataObject %llu", (unsigned long long)data_id);
                #endif
                return delete_data_object(data_id);
            }

            return SUCCESS(); 
        }
        irods::error update_replica_access_time(data_id_t data_id, uint32_t repl_num, std::string_view time) { 
            std::string local_uuid = std::to_string(data_id) + ":" + std::to_string(repl_num);
            snowflake_id_t rid = SnowflakeID::create(local_cluster_id_, local_uuid);
            std::string payload = client_->get_node_payload_async(local_cluster_id_, rid).get();
            if (!payload.empty()) {
                 lite3cpp::Buffer buf(std::vector<uint8_t>(payload.begin(), payload.end()));
                 buf.set_str(0, "at", std::string(time));
                 client_->put_node_async(local_cluster_id_, rid, buf.move_to_string()).get();
            }
            return SUCCESS(); 
        }
        uint32_t get_next_replica_number(data_id_t data_id) {
            snowflake_id_t sid = make_id(EntityType::DataObject, data_id);
            auto replicas = client_->get_neighbors_async(local_cluster_id_, sid, "HAS_REPLICA", 0.0).get();
            int64_t max_rn = -1;
            for (auto rid : replicas) {
                std::string payload = client_->get_node_payload_async(local_cluster_id_, rid).get();
                if (payload.empty()) continue;
                lite3cpp::Buffer buf(std::vector<uint8_t>(payload.begin(), payload.end()));
                try {
                    int64_t rn = buf.get_i64(0, "rn");
                    if (rn > max_rn) max_rn = rn;
                } catch (...) {}
            }
            return static_cast<uint32_t>(max_rn + 1);
        }
        irods::error modify_replicas_for_data_object(data_id_t obj_id, uint32_t repl_num, const std::vector<std::pair<std::string, std::string>>& updates, bool all_repl_status) {
            snowflake_id_t sid = make_id(EntityType::DataObject, obj_id);
            auto replicas = client_->get_neighbors_async(local_cluster_id_, sid, "HAS_REPLICA", 0.0).get();
            for (auto rid : replicas) {
                std::string payload = client_->get_node_payload_async(local_cluster_id_, rid).get();
                if (payload.empty()) continue;
                lite3cpp::Buffer buf(std::vector<uint8_t>(payload.begin(), payload.end()));
                uint32_t rn = 0;
                try { rn = static_cast<uint32_t>(buf.get_i64(0, "rn")); } catch (...) {}

                bool modified = false;
                if (all_repl_status) {
                    if (rn == repl_num) {
                        buf.set_str(0, "st", "1"); // GOOD_REPLICA
                    } else {
                        buf.set_str(0, "st", "0"); // STALE_REPLICA
                    }
                    modified = true;
                }

                if (rn == repl_num || replicas.size() == 1) {
                    for (const auto& [kw, val] : updates) {
                        if (kw == "dataModify" || kw == "modify_ts") {
                            buf.set_str(0, "mt", val);
                            modified = true;
                        } else if (kw == "chksum" || kw == "data_checksum") {
                            buf.set_str(0, "cs", val);
                            modified = true;
                        } else if (kw == "filePath" || kw == "data_path") {
                            buf.set_str(0, "p", val);
                            modified = true;
                        } else if (kw == "rescHier" || kw == "resc_hier") {
                            buf.set_str(0, "rh", val);
                            modified = true;
                        } else if (kw == "replStatus" || kw == "data_is_dirty") {
                            if (!all_repl_status) {
                                buf.set_str(0, "st", val);
                                modified = true;
                            }
                        } else if (kw == "dataSize" || kw == "data_size" || kw == "DATA_SIZE" || kw == "size" || kw == "s") {
                            try {
                                buf.set_i64(0, "s", std::stoll(val));
                                modified = true;
                            } catch (...) {}
                        }
                    }
                }
                if (modified) {
                    client_->put_node_async(local_cluster_id_, rid, buf.move_to_string()).get();
                }
            }
            return SUCCESS();
        }

        // --- Collections ---
        irods::error register_collection(const collection& coll, coll_id_t& out_id) {
            // Check if collection already exists
            snowflake_id_t existing_sid = resolve_id_from_index(EntityType::Collection, "n", coll.name);
            if (existing_sid) {
                return ERROR(CATALOG_ALREADY_HAS_ITEM_BY_THAT_NAME, "Collection already exists: " + coll.name);
            }

            // Check if data object already exists with this path
            snowflake_id_t data_sid = resolve_id_from_index(EntityType::DataObject, "path", coll.name);
            if (!data_sid) {
                // Also check if data object exists in parent collection
                std::string_view coll_name = coll.name;
                size_t last_slash = coll_name.find_last_of('/');
                if (last_slash != std::string_view::npos && last_slash + 1 < coll_name.size()) {
                    std::string parent_coll = (last_slash == 0) ? "/" : std::string(coll_name.substr(0, last_slash));
                    std::string base_name = std::string(coll_name.substr(last_slash + 1));
                    snowflake_id_t psid = resolve_id_from_index(EntityType::Collection, "n", parent_coll);
                    if (psid) {
                        auto children = client_->get_neighbors_async(local_cluster_id_, psid, "CONTAINS", 0.0).get();
                        for (auto cid : children) {
                            std::string payload = client_->get_node_payload_async(local_cluster_id_, cid).get();
                            if (!payload.empty()) {
                                try {
                                    lite3cpp::Buffer buf(std::vector<uint8_t>(payload.begin(), payload.end()));
                                    std::string t = safe_get_str(buf, 0, "t");
                                    std::string name = safe_get_str(buf, 0, "n");
                                    std::string p = safe_get_str(buf, 0, "p");
                                    if (p == coll.name || (t != "collection" && name == base_name)) {
                                        data_sid = cid;
                                        break;
                                    }
                                } catch (...) {}
                            }
                        }
                    }
                }
            }
            if (data_sid) {
                return ERROR(CAT_NAME_EXISTS_AS_DATAOBJ, "Data object already exists with collection name: " + coll.name);
            }

            std::string parent_name = coll.parent_name;
            if (parent_name.empty() && coll.name != "/" && !coll.name.empty()) {
                size_t last_slash = coll.name.find_last_of('/');
                if (last_slash != std::string::npos) {
                    parent_name = (last_slash == 0) ? "/" : coll.name.substr(0, last_slash);
                }
            }

            snowflake_id_t sid = make_id(EntityType::Collection, coll.id);
            #ifdef IRODS_SERVER
            rodsLog(LOG_NOTICE, "L3_CATALOG: Registering Collection [%s] with ID [%llu] (SID: %016llx)", coll.name.c_str(), (unsigned long long)coll.id, (unsigned long long)sid);
            #endif
            lite3cpp::Buffer buf; buf.init_object(); 
            buf.set_str(0, "n", coll.name); 
            buf.set_str(0, "pn", parent_name);
            buf.set_str(0, "o", coll.owner_name); 
            buf.set_str(0, "z", coll.owner_zone); 
            buf.set_str(0, "t", coll.type);
            buf.set_str(0, "entity_type", "collection");
            buf.set_str(0, "ct", coll.create_ts);
            buf.set_str(0, "mt", coll.modify_ts);
            buf.set_str(0, "c1", coll.info1);
            buf.set_str(0, "c2", coll.info2);
            buf.set_str(0, "i", coll.inheritance);
            buf.set_str(0, "m", coll.comments);
            buf.set_i64(0, "id", static_cast<int64_t>(coll.id));
            client_->put_node_async(local_cluster_id_, sid, buf.move_to_string()).get();
            add_index(EntityType::Collection, "n", coll.name, sid);
            add_index(EntityType::Collection, "id", std::to_string(coll.id), sid);

            snowflake_id_t psid = 0;
            if (coll.parent_id != 0) {
                psid = make_id(EntityType::Collection, coll.parent_id);
            } else if (!parent_name.empty() && coll.name != "/") {
                psid = resolve_id_from_index(EntityType::Collection, "n", parent_name);
            }

            if (psid != 0) {
                #ifdef IRODS_SERVER
                rodsLog(LOG_NOTICE, "L3_CATALOG: Creating CONTAINS edge (Coll-to-Coll): %016llx -- CONTAINS --> %016llx", (unsigned long long)psid, (unsigned long long)sid);
                #endif
                add_edge(psid, "CONTAINS", 1.0, sid);
            } else {
                snowflake_id_t zid = make_id(EntityType::Zone, 1);
                add_edge(zid, "HAS_ROOT_COLL", 1.0, sid);
            }
            
            // Implicitly grant 'own' access to the creator
            auto access_ret = set_access(coll.owner_name, coll.owner_zone, coll.name, "own", false);
            if (!access_ret.ok()) {
                #ifdef IRODS_SERVER
                rodsLog(LOG_NOTICE, "L3_CATALOG: register_collection failed to set owner access for [%s]: %s", coll.name.c_str(), access_ret.result().c_str());
                #endif
            }
            
            out_id = coll.id; return SUCCESS();
        }
        void update_collection_subtree(snowflake_id_t coll_sid, const std::string& old_prefix, const std::string& new_prefix, const std::string& new_parent_path) {
            std::string payload = client_->get_node_payload_async(local_cluster_id_, coll_sid).get();
            char time_buf[50];
            snprintf(time_buf, sizeof(time_buf), "%011lld", (long long)time(nullptr));

            if (!payload.empty()) {
                lite3cpp::Buffer buf(std::vector<uint8_t>(payload.begin(), payload.end()));
                buf.set_str(0, "n", new_prefix);
                buf.set_str(0, "pn", new_parent_path);
                buf.set_str(0, "mt", std::string(time_buf));
                
                client_->put_node_async(local_cluster_id_, coll_sid, buf.move_to_string()).get();
                del_index(EntityType::Collection, "n", old_prefix);
                add_index(EntityType::Collection, "n", new_prefix, coll_sid);
            }

            auto children = client_->get_neighbors_async(local_cluster_id_, coll_sid, "CONTAINS", 0.0).get();
            for (snowflake_id_t child_sid : children) {
                std::string ch_payload = client_->get_node_payload_async(local_cluster_id_, child_sid).get();
                if (ch_payload.empty()) continue;
                lite3cpp::Buffer ch_buf(std::vector<uint8_t>(ch_payload.begin(), ch_payload.end()));
                std::string ch_type = safe_get_str(ch_buf, 0, "t");
                std::string ch_entity_type = safe_get_str(ch_buf, 0, "entity_type");
                std::string ch_pn = safe_get_str(ch_buf, 0, "pn");
                bool is_coll = (ch_entity_type == "collection" || ch_type == "collection" || !ch_pn.empty());
                if (is_coll) {
                    std::string old_ch_name = safe_get_str(ch_buf, 0, "n");
                    std::string sub = (old_ch_name.size() >= old_prefix.size()) ? old_ch_name.substr(old_prefix.size()) : "";
                    std::string new_ch_name = new_prefix + sub;
                    update_collection_subtree(child_sid, old_ch_name, new_ch_name, new_prefix);
                } else {
                    std::string old_do_path = safe_get_str(ch_buf, 0, "p");
                    std::string do_name = safe_get_str(ch_buf, 0, "n");
                    std::string new_do_path = (new_prefix == "/" ? "/" : new_prefix + "/") + do_name;
                    
                    ch_buf.set_str(0, "p", new_do_path);
                    ch_buf.set_str(0, "mt", std::string(time_buf));
                    
                    client_->put_node_async(local_cluster_id_, child_sid, ch_buf.move_to_string()).get();
                    if (!old_do_path.empty()) {
                        del_index(EntityType::DataObject, "path", old_do_path);
                    }
                    add_index(EntityType::DataObject, "path", new_do_path, child_sid);
                }
            }
        }

        void get_collection_subtree_ids(snowflake_id_t coll_sid, std::vector<snowflake_id_t>& out_ids, std::unordered_set<snowflake_id_t>& visited) {
            if (!visited.insert(coll_sid).second) return;
            out_ids.push_back(coll_sid);
            auto children = client_->get_neighbors_async(local_cluster_id_, coll_sid, "CONTAINS", 0.0).get();
            for (snowflake_id_t child_sid : children) {
                std::string ch_payload = client_->get_node_payload_async(local_cluster_id_, child_sid).get();
                if (ch_payload.empty()) continue;
                lite3cpp::Buffer ch_buf(std::vector<uint8_t>(ch_payload.begin(), ch_payload.end()));
                std::string ch_type = safe_get_str(ch_buf, 0, "t");
                std::string ch_entity_type = safe_get_str(ch_buf, 0, "entity_type");
                std::string ch_pn = safe_get_str(ch_buf, 0, "pn");
                bool is_coll = (ch_entity_type == "collection" || ch_type == "collection" || !ch_pn.empty());
                if (is_coll) {
                    get_collection_subtree_ids(child_sid, out_ids, visited);
                }
            }
        }

        irods::error get_collection_subtree_ids(snowflake_id_t coll_sid, std::vector<snowflake_id_t>& out_ids) {
            std::unordered_set<snowflake_id_t> visited;
            get_collection_subtree_ids(coll_sid, out_ids, visited);
            return SUCCESS();
        }

        irods::error rename_collection(std::string_view old_name, std::string_view new_name) { 
            snowflake_id_t sid = resolve_id_from_index(EntityType::Collection, "n", old_name);
            if (!sid) return ERROR(CAT_UNKNOWN_COLLECTION, "Collection not found");
            
            irods::experimental::filesystem::path p(std::string{new_name});
            std::string new_pn = p.parent_path().string();
            if (new_pn.empty()) new_pn = "/";

            irods::experimental::filesystem::path old_p(std::string{old_name});
            std::string old_pn = old_p.parent_path().string();
            if (old_pn.empty()) old_pn = "/";

            if (old_pn != new_pn) {
                snowflake_id_t old_psid = resolve_id_from_index(EntityType::Collection, "n", old_pn);
                snowflake_id_t new_psid = resolve_id_from_index(EntityType::Collection, "n", new_pn);
                if (old_psid) del_edge(old_psid, "CONTAINS", 1.0, sid);
                if (new_psid) add_edge(new_psid, "CONTAINS", 1.0, sid);
            }

            update_collection_subtree(sid, std::string(old_name), std::string(new_name), new_pn);
            return SUCCESS(); 
        }

        irods::error rename_collection_by_id(coll_id_t coll_id, std::string_view new_name) {
            snowflake_id_t cid = make_id(EntityType::Collection, coll_id);
            std::string payload = client_->get_node_payload_async(local_cluster_id_, cid).get();
            if (payload.empty()) {
                return ERROR(CAT_UNKNOWN_COLLECTION, "Collection not found");
            }
            lite3cpp::Buffer buf(std::vector<uint8_t>(payload.begin(), payload.end()));
            std::string old_coll_name = safe_get_str(buf, 0, "n");
            std::string parent_coll_name = safe_get_str(buf, 0, "pn");

            std::string new_coll_name;
            std::string new_pn = parent_coll_name;
            if (new_name.starts_with("/")) {
                new_coll_name = std::string(new_name);
                size_t slash = new_coll_name.find_last_of('/');
                new_pn = (slash == 0) ? "/" : new_coll_name.substr(0, slash);
            } else {
                new_coll_name = (parent_coll_name == "/" ? "/" : parent_coll_name + "/") + std::string(new_name);
            }

            if (new_pn != parent_coll_name) {
                snowflake_id_t old_psid = resolve_id_from_index(EntityType::Collection, "n", parent_coll_name);
                snowflake_id_t new_psid = resolve_id_from_index(EntityType::Collection, "n", new_pn);
                if (old_psid) del_edge(old_psid, "CONTAINS", 1.0, cid);
                if (new_psid) add_edge(new_psid, "CONTAINS", 1.0, cid);
            }

            update_collection_subtree(cid, old_coll_name, new_coll_name, new_pn);
            return SUCCESS();
        }

        irods::error move_collection(coll_id_t coll_id, coll_id_t target_coll_id) {
            snowflake_id_t cid = make_id(EntityType::Collection, coll_id);
            std::string payload = client_->get_node_payload_async(local_cluster_id_, cid).get();
            if (payload.empty()) {
                return ERROR(CAT_UNKNOWN_COLLECTION, "Source collection not found");
            }

            snowflake_id_t target_cid = make_id(EntityType::Collection, target_coll_id);
            std::string target_payload = client_->get_node_payload_async(local_cluster_id_, target_cid).get();
            if (target_payload.empty()) {
                return ERROR(CAT_UNKNOWN_COLLECTION, "Target collection not found");
            }

            lite3cpp::Buffer buf(std::vector<uint8_t>(payload.begin(), payload.end()));
            lite3cpp::Buffer tbuf(std::vector<uint8_t>(target_payload.begin(), target_payload.end()));

            std::string old_coll_name = safe_get_str(buf, 0, "n");
            std::string old_parent_name = safe_get_str(buf, 0, "pn");
            std::string target_parent_name = safe_get_str(tbuf, 0, "n");

            size_t last_slash = old_coll_name.find_last_of('/');
            std::string coll_basename = (last_slash != std::string::npos) ? old_coll_name.substr(last_slash + 1) : old_coll_name;

            std::string new_coll_name = (target_parent_name == "/" ? "/" : target_parent_name + "/") + coll_basename;

            if (target_parent_name == old_coll_name || target_parent_name.starts_with(old_coll_name + "/")) {
                return ERROR(CAT_RECURSIVE_MOVE, "Cannot move collection into its own subtree");
            }

            auto in_colls = client_->get_in_neighbors_async(local_cluster_id_, cid, "CONTAINS").get();
            for (auto old_psid : in_colls) {
                del_edge(old_psid, "CONTAINS", 1.0, cid);
            }

            add_edge(target_cid, "CONTAINS", 1.0, cid);
            update_collection_subtree(cid, old_coll_name, new_coll_name, target_parent_name);
            return SUCCESS();
        }

        irods::error rename_object(uint64_t obj_id, std::string_view new_name) {
            snowflake_id_t cid = make_id(EntityType::Collection, obj_id);
            std::string c_payload = client_->get_node_payload_async(local_cluster_id_, cid).get();
            snowflake_id_t did = make_id(EntityType::DataObject, obj_id);
            std::string d_payload = client_->get_node_payload_async(local_cluster_id_, did).get();

            if (!c_payload.empty() && !d_payload.empty()) {
                lite3cpp::Buffer cbuf(std::vector<uint8_t>(c_payload.begin(), c_payload.end()));
                lite3cpp::Buffer dbuf(std::vector<uint8_t>(d_payload.begin(), d_payload.end()));
                std::string cname = safe_get_str(cbuf, 0, "n");
                std::string dpath = safe_get_str(dbuf, 0, "p");
                if (new_name.starts_with("/")) {
                    size_t c_match = 0;
                    while (c_match < new_name.size() && c_match < cname.size() && new_name[c_match] == cname[c_match]) c_match++;
                    size_t d_match = 0;
                    while (d_match < new_name.size() && d_match < dpath.size() && new_name[d_match] == dpath[d_match]) d_match++;
                    if (c_match >= d_match) return rename_collection_by_id(obj_id, new_name);
                    else return rename_data_object(obj_id, new_name);
                }
                return rename_collection_by_id(obj_id, new_name);
            }
            if (!c_payload.empty()) {
                return rename_collection_by_id(obj_id, new_name);
            }
            if (!d_payload.empty()) {
                return rename_data_object(obj_id, new_name);
            }
            return ERROR(CAT_UNKNOWN_COLLECTION, "Object not found for rename: " + std::to_string(obj_id));
        }

        irods::error move_object(uint64_t obj_id, uint64_t target_coll_id) {
            snowflake_id_t cid = make_id(EntityType::Collection, obj_id);
            std::string c_payload = client_->get_node_payload_async(local_cluster_id_, cid).get();
            snowflake_id_t did = make_id(EntityType::DataObject, obj_id);
            std::string d_payload = client_->get_node_payload_async(local_cluster_id_, did).get();

            if (!c_payload.empty() && !d_payload.empty()) {
                lite3cpp::Buffer cbuf(std::vector<uint8_t>(c_payload.begin(), c_payload.end()));
                lite3cpp::Buffer dbuf(std::vector<uint8_t>(d_payload.begin(), d_payload.end()));
                snowflake_id_t target_cid = make_id(EntityType::Collection, target_coll_id);
                std::string target_payload = client_->get_node_payload_async(local_cluster_id_, target_cid).get();
                if (!target_payload.empty()) {
                    lite3cpp::Buffer tbuf(std::vector<uint8_t>(target_payload.begin(), target_payload.end()));
                    std::string tpath = safe_get_str(tbuf, 0, "n");
                    std::string cpath = safe_get_str(cbuf, 0, "n");
                    std::string dpath = safe_get_str(dbuf, 0, "p");
                    size_t c_match = 0;
                    while (c_match < tpath.size() && c_match < cpath.size() && tpath[c_match] == cpath[c_match]) c_match++;
                    size_t d_match = 0;
                    while (d_match < tpath.size() && d_match < dpath.size() && tpath[d_match] == dpath[d_match]) d_match++;
                    if (c_match >= d_match) {
                        return move_collection(obj_id, target_coll_id);
                    } else {
                        return move_data_object(obj_id, target_coll_id);
                    }
                }
            }
            if (!c_payload.empty()) {
                return move_collection(obj_id, target_coll_id);
            }
            if (!d_payload.empty()) {
                return move_data_object(obj_id, target_coll_id);
            }
            return ERROR(CAT_UNKNOWN_COLLECTION, "Object not found for move: " + std::to_string(obj_id));
        }
        irods::error delete_collection(coll_id_t coll_id) { 
            snowflake_id_t sid = make_id(EntityType::Collection, coll_id);
            std::string payload = client_->get_node_payload_async(local_cluster_id_, sid).get();
            if (payload.empty()) {
                std::string direct_payload = client_->get_node_payload_async(local_cluster_id_, coll_id).get();
                if (!direct_payload.empty()) {
                    sid = coll_id;
                    payload = direct_payload;
                }
            }
            
            #ifdef IRODS_SERVER
            rodsLog(LOG_NOTICE, "L3_CATALOG: Deleting Collection %llu (SID: %016llx)", (unsigned long long)coll_id, (unsigned long long)sid);
            #endif

            // Delete path index and edges
            if (!payload.empty()) {
                try {
                    lite3cpp::Buffer buf(std::vector<uint8_t>(payload.begin(), payload.end()));
                    std::string path = safe_get_str(buf, 0, "n");
                    del_index(EntityType::Collection, "n", path);
                    std::string id_str = safe_get_str(buf, 0, "id");
                    if (!id_str.empty()) {
                        del_index(EntityType::Collection, "id", id_str);
                    } else if (coll_id != 0) {
                        del_index(EntityType::Collection, "id", std::to_string(coll_id));
                    }

                    // Delete incoming edges (CONTAINS, OWNS)
                    auto collections = client_->get_in_neighbors_async(local_cluster_id_, sid, "CONTAINS").get();
                    for (auto cid : collections) {
                        del_edge(cid, "CONTAINS", 1.0, sid);
                    }
                    auto owners = client_->get_in_neighbors_async(local_cluster_id_, sid, "OWNS").get();
                    for (auto oid : owners) {
                        del_edge(oid, "OWNS", 1.0, sid);
                    }
                    auto accesses = client_->get_in_neighbors_async(local_cluster_id_, sid, "FOR_OBJECT").get();
                    for (auto aid : accesses) {
                        del_edge(aid, "FOR_OBJECT", 1.0, sid);
                        client_->del_node_async(local_cluster_id_, aid).get();
                    }
                } catch (...) {}
            }
            client_->del_node_async(local_cluster_id_, sid).get();
            return SUCCESS(); 
        }
        irods::error modify_collection(coll_id_t coll_id, std::string_view prop, std::string_view value) { 
            snowflake_id_t sid = make_id(EntityType::Collection, coll_id);
            std::string payload = client_->get_node_payload_async(local_cluster_id_, sid).get();
            if (payload.empty()) {
                payload = client_->get_node_payload_async(local_cluster_id_, coll_id).get();
                if (!payload.empty()) {
                    sid = coll_id;
                } else {
                    return ERROR(-1, "Collection not found for modify");
                }
            }
            lite3cpp::Buffer buf(std::vector<uint8_t>(payload.begin(), payload.end()));
            
            std::string key(prop);
            if (prop == "inheritance") key = "i";
            else if (prop == "comment" || prop == "coll_comments" || prop == "collComments") key = "m";
            else if (prop == "type" || prop == "coll_type" || prop == "collType" || prop == "collectionType") key = "t";
            else if (prop == "info1" || prop == "coll_info1" || prop == "collInfo1" || prop == "collectionInfo1") key = "c1";
            else if (prop == "info2" || prop == "coll_info2" || prop == "collInfo2" || prop == "collectionInfo2") key = "c2";
            else if (prop == "modify_ts" || prop == "collModify" || prop == "collectionMtime" || prop == "mtime") key = "mt";
            
            buf.set_str(0, key, std::string(value));
            client_->put_node_async(local_cluster_id_, sid, buf.move_to_string()).get();
            return SUCCESS(); 
        }

        // --- Resources ---
        irods::error register_resource(const resource& resc, resc_id_t& out_id) {
            snowflake_id_t sid = make_id(EntityType::Resource, resc.id);
            lite3cpp::Buffer buf(4096); buf.init_object(); 
            buf.set_i64(0, "id", static_cast<int64_t>(resc.id)); 
            buf.set_str(0, "n", resc.name); 
            buf.set_str(0, "z", local_zone_name_); 
            buf.set_str(0, "t", resc.type); 
            buf.set_str(0, "l", resc.location);
            buf.set_str(0, "v", resc.vault_path);
            buf.set_str(0, "cx", resc.context);
            buf.set_str(0, "m", resc.comments);
            buf.set_i64(0, "f", resc.free_space);
            buf.set_i64(0, "s", static_cast<int64_t>(resc.status));
            buf.set_str(0, "ct", resc.create_ts);
            buf.set_str(0, "mt", resc.modify_ts);
            client_->put_node_async(local_cluster_id_, sid, buf.move_to_string()).get();
            add_index(EntityType::Resource, "n", resc.name, sid);
            add_index(EntityType::Resource, "id", std::to_string(resc.id), sid);
            snowflake_id_t zid = make_id(EntityType::Zone, 1);
            add_edge(zid, "HAS_RESC", 1.0, sid);
            out_id = sid; return SUCCESS();
        }
        irods::error modify_resource(snowflake_id_t sid, std::string_view prop, std::string_view value) { 
            std::string payload = client_->get_node_payload_async(local_cluster_id_, sid).get();
            if (!payload.empty()) {
                 lite3cpp::Buffer buf(std::vector<uint8_t>(payload.begin(), payload.end()));
                 
                 std::string key(prop);
                 if (prop == "path" || prop == "resc_def_path") key = "v";
                 else if (prop == "host" || prop == "location" || prop == "resc_net") key = "l";
                 else if (prop == "type" || prop == "resc_type_name") key = "t";
                 else if (prop == "free_space") key = "f";
                 else if (prop == "comment" || prop == "r_comment") key = "m";
                 else if (prop == "status" || prop == "resc_status") key = "s";
                 else if (prop == "context" || prop == "resc_context") key = "cx";
                 
                 try {
                     if (key == "f" || key == "s") {
                         buf.set_i64(0, key, std::stoll(std::string(value)));
                     } else {
                         buf.set_str(0, key, std::string(value));
                     }
                 } catch (...) {
                     buf.set_str(0, key, std::string(value));
                 }
                 
                 client_->put_node_async(local_cluster_id_, sid, buf.move_to_string()).get();
            }
            return SUCCESS(); 
        }
        irods::error delete_resource(snowflake_id_t sid) { 
            // Delete name index
            std::string payload = client_->get_node_payload_async(local_cluster_id_, sid).get();
            if (!payload.empty()) {
                try {
                    lite3cpp::Buffer buf(std::vector<uint8_t>(payload.begin(), payload.end()));
                    std::string name = safe_get_str(buf, 0, "n");
                    del_index(EntityType::Resource, "n", name);
                    std::string id_str = safe_get_str(buf, 0, "id");
                    if (!id_str.empty()) del_index(EntityType::Resource, "id", id_str);
                } catch (...) {}
            }
            client_->del_node_async(local_cluster_id_, sid).get();
            return SUCCESS(); 
        }
        irods::error resolve_resource_name(std::string_view name, snowflake_id_t& out_id) { 
            snowflake_id_t sid = resolve_id_from_index(EntityType::Resource, "n", name);
            if (!sid) return ERROR(-1, "Resource not found");
            out_id = sid;
            return SUCCESS(); 
        }
        irods::error resolve_user_name(std::string_view name, snowflake_id_t& out_id) { 
            snowflake_id_t sid = resolve_id_from_index(EntityType::User, "n", name);
            if (!sid) return ERROR(-1, "User not found");
            out_id = sid;
            return SUCCESS(); 
        }
        irods::error get_hierarchy_for_resource(std::string_view name, std::string& out_hier) {
            snowflake_id_t sid = resolve_id_from_index(EntityType::Resource, "n", name);
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
                        parts.insert(parts.begin(), safe_get_str(buf, 0, "n"));
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
            snowflake_id_t pid = resolve_id_from_index(EntityType::Resource, "n", parent_name);
            snowflake_id_t cid = resolve_id_from_index(EntityType::Resource, "n", child_name);
            if (!pid || !cid) return ERROR(-1, "Parent or child resource not found");
            
            add_edge(pid, "HAS_CHILD", 1.0, cid);
            // Optionally store context on the edge or child node.
            return SUCCESS(); 
        }
        irods::error remove_child_resource(std::string_view parent_name, std::string_view child_name) { 
            snowflake_id_t pid = resolve_id_from_index(EntityType::Resource, "n", parent_name);
            snowflake_id_t cid = resolve_id_from_index(EntityType::Resource, "n", child_name);
            if (!pid || !cid) return ERROR(-1, "Parent or child resource not found");
            
            std::string edge_key = std::string(l3kvg::KeyBuilder::edge_out_key(pid, "HAS_CHILD", 1.0, cid));
            client_->del_edge_async(local_cluster_id_, edge_key).get();
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
                 client_->put_node_async(local_cluster_id_, sid, buf.move_to_string()).get();
            }
            return SUCCESS(); 
        }

        // --- Identity ---
        irods::error register_user(const user& usr, user_id_t& out_id) {
            snowflake_id_t existing_sid = resolve_id_from_index(EntityType::User, "n", usr.name);
            snowflake_id_t sid = make_id(EntityType::User, usr.id);
            if (existing_sid && existing_sid != sid) {
                snowflake_id_t zid = make_id(EntityType::Zone, 1);
                del_edge(zid, "HAS_USER", 1.0, existing_sid);
                client_->del_node_async(local_cluster_id_, existing_sid).get();
            }
            int priv = (usr.type == "rodsadmin" ? 5 : 1);
            #ifdef IRODS_SERVER
            rodsLog(LOG_NOTICE, "L3_CATALOG: Registering User [%s] type [%s] id [%llu] priv [%d]", usr.name.c_str(), usr.type.c_str(), (unsigned long long)usr.id, priv);
            #endif
            lite3cpp::Buffer buf; buf.init_object(); 
            buf.set_str(0, "n", usr.name);
            buf.set_str(0, "t", usr.type);
            buf.set_str(0, "z", usr.zone);
            buf.set_i64(0, "p", priv);
            buf.set_i64(0, "id", static_cast<int64_t>(usr.id));
            client_->put_node_async(local_cluster_id_, sid, buf.move_to_string()).get();
            add_index(EntityType::User, "n", usr.name, sid);
            add_index(EntityType::User, "id", std::to_string(usr.id), sid);
            snowflake_id_t zid = make_id(EntityType::Zone, 1);
            add_edge(zid, "HAS_USER", 1.0, sid);
            out_id = usr.id; return SUCCESS();
        }
        irods::error delete_user(std::string_view user_name) { 
            snowflake_id_t uid = resolve_id_from_index(EntityType::User, "n", user_name);
            if (uid) {
                snowflake_id_t zid = make_id(EntityType::Zone, 1);
                del_edge(zid, "HAS_USER", 1.0, uid);
                del_index(EntityType::User, "n", user_name);
                std::string payload = client_->get_node_payload_async(local_cluster_id_, uid).get();
                if (!payload.empty()) {
                    try {
                        lite3cpp::Buffer buf(std::vector<uint8_t>(payload.begin(), payload.end()));
                        std::string id_str = safe_get_str(buf, 0, "id");
                        if (!id_str.empty()) del_index(EntityType::User, "id", id_str);
                    } catch (...) {}
                }
                client_->del_node_async(local_cluster_id_, uid).get();
            }
            return SUCCESS(); 
        }
        irods::error modify_user(std::string_view user_name, std::string_view prop, std::string_view value) { 
            snowflake_id_t uid = resolve_id_from_index(EntityType::User, "n", user_name);
            if (!uid) return ERROR(-1, "User not found");
            
            std::string payload = client_->get_node_payload_async(local_cluster_id_, uid).get();
            if (!payload.empty()) {
                 lite3cpp::Buffer buf(std::vector<uint8_t>(payload.begin(), payload.end()));
                 if (prop == "type") {
                     buf.set_i64(0, "p", (value == "rodsadmin" ? 5 : 1));
                     buf.set_str(0, "t", std::string(value));
                 } else if (prop == "password") {
                     buf.set_str(0, "pw", value);
                 } else if (prop == "comment") {
                     buf.set_str(0, "c", std::string(value));
                 } else if (prop == "info") {
                     buf.set_str(0, "info", std::string(value));
                 }
                 client_->put_node_async(local_cluster_id_, uid, buf.move_to_string()).get();
            }
            return SUCCESS(); 
        }
        irods::error check_auth(std::string_view user_name, std::string_view zone, int& user_priv) {
            snowflake_id_t uid = resolve_id_from_index(EntityType::User, "n", user_name);
            if (!uid) return ERROR(-1, "User not found");
            auto fut = client_->get_node_payload_async(local_cluster_id_, uid);
            std::string payload = fut.get();
            if (payload.empty()) return ERROR(-1, "User node missing");
            try {
                lite3cpp::Buffer buf(std::vector<uint8_t>(payload.begin(), payload.end()));
                user_priv = static_cast<int>(buf.get_i64(0, "p"));
                #ifdef IRODS_SERVER
                rodsLog(LOG_NOTICE, "L3_CATALOG: check_auth user=[%s] priv=[%d]", user_name.data(), user_priv);
                #endif
                return SUCCESS();
            } catch (...) { return ERROR(-1, "Failed to parse priv level"); }
        }
        irods::error get_user_password_and_priv(std::string_view user_name, std::string_view zone, std::string& out_pw, int& out_priv) {
            snowflake_id_t uid = resolve_id_from_index(EntityType::User, "n", user_name);
            if (!uid) return ERROR(-1, "User not found");
            auto fut = client_->get_node_payload_async(local_cluster_id_, uid);
            std::string payload = fut.get();
            if (payload.empty()) return ERROR(-1, "User node missing");
            try {
                lite3cpp::Buffer buf(std::vector<uint8_t>(payload.begin(), payload.end()));
                out_priv = static_cast<int>(buf.get_i64(0, "p"));
                if (buf.get_type(0, "pw") != lite3cpp::Type::Invalid) {
                    out_pw = safe_get_str(buf, 0, "pw");
                } else {
                    out_pw = "";
                }
                return SUCCESS();
            } catch (...) {
                return ERROR(-1, "Failed to read user payload");
            }
        }
        irods::error check_auth_credentials(std::string_view username, std::string_view zone, std::string_view password, bool& correct) {
            correct = false;
            snowflake_id_t uid = resolve_id_from_index(EntityType::User, "n", username);
            if (!uid) return SUCCESS();

            std::string payload = client_->get_node_payload_async(local_cluster_id_, uid).get();
            if (!payload.empty()) {
                try {
                    lite3cpp::Buffer buf(std::vector<uint8_t>(payload.begin(), payload.end()));
                    if (buf.get_type(0, "pw") != lite3cpp::Type::Invalid) {
                        std::string stored_pw = safe_get_str(buf, 0, "pw");
                        correct = (stored_pw == password);
                    } else {
                        correct = false;
                    }
                } catch (...) {}
            }
            return SUCCESS();
        }
        irods::error add_user_to_group(std::string_view user_name, std::string_view zone, std::string_view group_name) { 
            snowflake_id_t uid = resolve_id_from_index(EntityType::User, "n", user_name);
            snowflake_id_t gid = resolve_id_from_index(EntityType::User, "n", group_name);
            if (!uid || !gid) return ERROR(-1, "User or group not found");
            add_edge(uid, "MEMBER_OF", 1.0, gid);
            return SUCCESS(); 
        }
        irods::error remove_user_from_group(std::string_view user_name, std::string_view zone, std::string_view group_name) { 
            snowflake_id_t uid = resolve_id_from_index(EntityType::User, "n", user_name);
            snowflake_id_t gid = resolve_id_from_index(EntityType::User, "n", group_name);
            if (!uid || !gid) return ERROR(-1, "User or group not found");
            
            std::string key = std::string(l3kvg::KeyBuilder::edge_out_key(uid, "MEMBER_OF", 1.0, gid));
            client_->del_edge_async(local_cluster_id_, key).get();
            return SUCCESS(); 
        }

        // --- ACLs ---
        irods::error set_access(std::string_view user_name, std::string_view zone, std::string_view target_path, std::string_view level, bool recursive) { 
            snowflake_id_t uid = resolve_id_from_index(EntityType::User, "n", user_name);
            if (!uid) return ERROR(-1, "User not found");
            
            // Resolve target path (could be data or collection)
            snowflake_id_t tid = 0;
            EntityType type;
            if (!resolve_path(target_path, tid, type).ok()) {
                return ERROR(-1, "Target path not found: " + std::string(target_path));
            }

            std::string aid_uuid = std::to_string(uid) + ":" + std::to_string(tid);
            snowflake_id_t aid = SnowflakeID::create(local_cluster_id_, aid_uuid);
            
            lite3cpp::Buffer buf; buf.init_object(); 
            buf.set_str(0, "l", std::string(level));
            buf.set_str(0, "t", "access_type");
            client_->put_node_async(local_cluster_id_, aid, buf.move_to_string()).get();
            
            add_edge(uid, "HAS_ACCESS", 1.0, aid);
            add_edge(aid, "FOR_OBJECT", 1.0, tid);
            
            return SUCCESS(); 
        }
        irods::error check_permission(snowflake_id_t user_sid, snowflake_id_t target_sid, std::string_view level_view, bool& allowed) { 
            std::string level(level_view);
            allowed = false;
            
            #ifdef IRODS_SERVER
            rodsLog(LOG_NOTICE, "L3_CATALOG: check_permission user_sid=%016llx target_sid=%016llx level=%s", (unsigned long long)user_sid, (unsigned long long)target_sid, level.c_str());
            #endif

            // Check if target exists
            std::string t_payload = client_->get_node_payload_async(local_cluster_id_, target_sid).get();
            if (t_payload.empty()) {
                #ifdef IRODS_SERVER
                rodsLog(LOG_NOTICE, "L3_CATALOG: check_permission target %016llx NOT FOUND - allowing for now", (unsigned long long)target_sid);
                #endif
                allowed = true;
                return SUCCESS(); // Target not found, let it proceed
            }

            // Check user priv level and extract username
            std::string user_name;
            std::string user_payload = client_->get_node_payload_async(local_cluster_id_, user_sid).get();
            if (!user_payload.empty()) {
                try {
                    lite3cpp::Buffer buf(std::vector<uint8_t>(user_payload.begin(), user_payload.end()));
                    user_name = safe_get_str(buf, 0, "n");
                    int priv = static_cast<int>(buf.get_i64(0, "p"));
                    if (priv >= 5) { // rodsadmin
                        allowed = true;
                        return SUCCESS();
                    }
                } catch (...) {}
            }

            // Target owner check fast-path
            try {
                lite3cpp::Buffer tbuf(std::vector<uint8_t>(t_payload.begin(), t_payload.end()));
                std::string target_owner = safe_get_str(tbuf, 0, "o");
                if (!user_name.empty() && user_name == target_owner) {
                    allowed = true;
                    return SUCCESS();
                }
            } catch (...) {}

            // Gather all principals (user + groups)
            std::vector<snowflake_id_t> principals = {user_sid};
            auto groups = client_->get_neighbors_async(local_cluster_id_, user_sid, "MEMBER_OF", 0.0).get();
            principals.insert(principals.end(), groups.begin(), groups.end());

            auto check_principal_access = [&](snowflake_id_t tid) -> bool {
                for (auto pid : principals) {
                    std::string aid_uuid = std::to_string(pid) + ":" + std::to_string(tid);
                    snowflake_id_t aid = SnowflakeID::create(local_cluster_id_, aid_uuid);
                    std::string payload = client_->get_node_payload_async(local_cluster_id_, aid).get();
                    if (!payload.empty()) {
                        try {
                            lite3cpp::Buffer buf(std::vector<uint8_t>(payload.begin(), payload.end()));
                            std::string actual_level = safe_get_str(buf, 0, "l");
                            if (actual_level.starts_with("admin:")) actual_level = actual_level.substr(6);

                            // Strict level check
                            if (actual_level == "own") return true;
                            if (actual_level == "write" && (level == "write" || level == "read")) return true;
                            if (actual_level == "read" && level == "read") return true;
                        } catch (...) {}
                    }
                }
                return false;
            };

            // Direct check on target_sid
            if (check_principal_access(target_sid)) {
                allowed = true;
                return SUCCESS();
            }

            // Check parent collection if target is in a collection (e.g. data object or subcollection)
            auto parents = client_->get_in_neighbors_async(local_cluster_id_, target_sid, "CONTAINS").get();
            for (auto psid : parents) {
                // Check parent owner
                std::string p_payload = client_->get_node_payload_async(local_cluster_id_, psid).get();
                if (!p_payload.empty()) {
                    try {
                        lite3cpp::Buffer pbuf(std::vector<uint8_t>(p_payload.begin(), p_payload.end()));
                        std::string p_owner = safe_get_str(pbuf, 0, "o");
                        if (!user_name.empty() && user_name == p_owner) {
                            allowed = true;
                            return SUCCESS();
                        }
                    } catch (...) {}
                }
                // Check parent access
                if (check_principal_access(psid)) {
                    allowed = true;
                    return SUCCESS();
                }
            }

            return SUCCESS(); 
        }
        irods::error check_permission_to_modify_data_object(snowflake_id_t user_sid, snowflake_id_t target_sid, bool& allowed) {
            return check_permission(user_sid, target_sid, "write", allowed);
        }

        void set_sequence_value(std::string_view seq_name, uint64_t val) {
            snowflake_id_t sid = SnowflakeID::create(local_cluster_id_, "seq:" + std::string(seq_name));
            lite3cpp::Buffer buf; buf.init_object(); buf.set_i64(0, "v", static_cast<int64_t>(val));
            client_->put_node_async(local_cluster_id_, sid, buf.move_to_string()).get();
        }

        irods::error get_next_sequence_value(std::string_view seq_name, uint64_t& out_val) {
            std::string effective_seq = std::string(seq_name);
            if (seq_name == "R_DATA_MAIN" || seq_name == "R_COLL_MAIN" || seq_name == "R_RESC_MAIN" || seq_name == "R_USER_MAIN" || seq_name == "R_RULE_EXEC" || seq_name == "R_OBJECTID") {
                effective_seq = "R_OBJECTID";
            }
            std::string key = "seq:" + effective_seq;
            out_val = client_->atomic_incr_async(local_cluster_id_, key, 1).get();
            if (out_val == 0) return ERROR(-1, "Failed to increment sequence: " + std::string(seq_name));
            if (effective_seq == "R_OBJECTID" && out_val < 100000) {
                uint64_t jump = 100000 - out_val;
                out_val = client_->atomic_incr_async(local_cluster_id_, key, jump).get();
            }
            return SUCCESS();
        }

        l3kvg::RemoteL3KVClient* get_client() const { return client_.get(); }
        uint16_t get_cluster_id() const { return local_cluster_id_; }

        // --- Metadata ---
        irods::error add_avu_metadata(std::string_view type, std::string_view target_id, const avu& metadata) {
            std::string local_uuid = metadata.attribute + ":" + metadata.value + ":" + metadata.units;
            snowflake_id_t aid = SnowflakeID::create(local_cluster_id_, local_uuid);
            lite3cpp::Buffer buf; buf.init_object(); buf.set_str(0, "a", metadata.attribute); buf.set_str(0, "v", metadata.value); buf.set_str(0, "u", metadata.units);
            client_->put_node_async(local_cluster_id_, aid, buf.move_to_string()).get();
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
            client_->del_edge_async(local_cluster_id_, edge_key).get();
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
            client_->put_node_async(local_cluster_id_, zid, buf.move_to_string()).get();
            add_index(EntityType::Zone, "n", z.name, zid);
            return SUCCESS();
        }
        irods::error modify_zone(std::string_view name, std::string_view prop, std::string_view value) { 
            snowflake_id_t zid = resolve_id_from_index(EntityType::Zone, "n", name);
            if (!zid) return ERROR(-1, "Zone not found");
            
            // In a graph we'd patch the node
            // For now, we don't have patch_str_async in RemoteL3KVClient, so we do full put
            std::string payload = client_->get_node_payload_async(local_cluster_id_, zid).get();
            if (!payload.empty()) {
                 lite3cpp::Buffer buf(std::vector<uint8_t>(payload.begin(), payload.end()));
                 buf.set_str(0, std::string(prop), std::string(value));
                 client_->put_node_async(local_cluster_id_, zid, buf.move_to_string()).get();
            }
            return SUCCESS(); 
        }
        irods::error delete_zone(std::string_view name) { 
            snowflake_id_t zid = resolve_id_from_index(EntityType::Zone, "n", name);
            if (zid) {
                del_index(EntityType::Zone, "n", name);
                client_->del_node_async(local_cluster_id_, zid).get();
            }
            return SUCCESS(); 
        }

        // --- Token & Quota ---
        irods::error register_token(std::string_view name, std::string_view value, std::string_view namespace_str) {
            snowflake_id_t sid = SnowflakeID::create(local_cluster_id_, std::string(namespace_str) + ":" + std::string(name));
            lite3cpp::Buffer buf; buf.init_object(); buf.set_str(0, "n", std::string(name)); buf.set_str(0, "v", std::string(value)); buf.set_str(0, "ns", std::string(namespace_str));
            client_->put_node_async(local_cluster_id_, sid, buf.move_to_string()).get();
            return SUCCESS();
        }
        irods::error delete_token(std::string_view name, std::string_view namespace_str) { 
            snowflake_id_t sid = SnowflakeID::create(local_cluster_id_, std::string(namespace_str) + ":" + std::string(name));
            client_->del_node_async(local_cluster_id_, sid).get();
            return SUCCESS(); 
        }
        irods::error set_quota(std::string_view user_name, std::string_view resc_name, int64_t limit) { 
            snowflake_id_t uid = resolve_id_from_index(EntityType::User, "n", user_name);
            if (!uid) return ERROR(-1, "User not found");

            std::string q_uuid = "quota:" + std::string(user_name) + ":" + std::string(resc_name);
            snowflake_id_t qid = SnowflakeID::create(local_cluster_id_, q_uuid);

            lite3cpp::Buffer buf; buf.init_object(); buf.set_i64(0, "limit", limit);
            client_->put_node_async(local_cluster_id_, qid, buf.move_to_string()).get();

            add_edge(uid, "HAS_QUOTA", 1.0, qid);
            return SUCCESS(); 
        }
        irods::error check_quota(std::string_view user_name, std::string_view resc_name, int64_t& usage, int64_t& limit) { 
            usage = 0; limit = -1;
            snowflake_id_t uid = resolve_id_from_index(EntityType::User, "n", user_name);
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
            usage = 0;
            snowflake_id_t uid = resolve_id_from_index(EntityType::User, "n", user_name);
            snowflake_id_t rid = resolve_id_from_index(EntityType::Resource, "n", resc_name);
            if (!uid || !rid) return SUCCESS();

            // 1. Find all DataObjects owned by user
            auto data_objects = client_->get_neighbors_async(local_cluster_id_, uid, "OWNS", 0.0).get();
            for (auto sid : data_objects) {
                // 2. Check if DataObject has a replica on this resource
                auto replicas = client_->get_neighbors_async(local_cluster_id_, sid, "HAS_REPLICA", 0.0).get();
                bool on_resc = false;
                for (auto rep_id : replicas) {
                    auto resources = client_->get_neighbors_async(local_cluster_id_, rep_id, "STAYING_AT", 0.0).get();
                    for (auto res_id : resources) {
                        if (res_id == rid) { on_resc = true; break; }
                    }
                    if (on_resc) break;
                }

                if (on_resc) {
                    // 3. Add size to total
                    std::string payload = client_->get_node_payload_async(local_cluster_id_, sid).get();
                    if (!payload.empty()) {
                        try {
                            lite3cpp::Buffer buf(std::vector<uint8_t>(payload.begin(), payload.end()));
                            usage += buf.get_i64(0, "s");
                        } catch (...) {}
                    }
                }
            }
            return SUCCESS(); 
        }

        // --- Rule Engine ---
        irods::error register_rule_execution(const rule_exec& re, uint64_t& out_id) {
            snowflake_id_t rid = make_id(EntityType::Rule, re.id);
            lite3cpp::Buffer buf; buf.init_object(); 
            buf.set_str(0, "n", re.name); buf.set_str(0, "e", re.exec_time); buf.set_str(0, "p", re.priority);
            client_->put_node_async(local_cluster_id_, rid, buf.move_to_string()).get();
            snowflake_id_t zid = make_id(EntityType::Zone, 1);
            add_edge(zid, "HAS_RULE", 1.0, rid);
            out_id = re.id; return SUCCESS();
        }
        irods::error delete_rule_execution(uint64_t id) {
            snowflake_id_t rid = make_id(EntityType::Rule, id);
            client_->del_node_async(local_cluster_id_, rid).get();
            return SUCCESS();
        }

        // --- Specific Query ---
        irods::error register_specific_query(std::string_view alias, std::string_view sql) {
            snowflake_id_t sid = SnowflakeID::create(local_cluster_id_, "sq:" + std::string(alias));
            lite3cpp::Buffer buf; buf.init_object(); buf.set_str(0, "a", std::string(alias)); buf.set_str(0, "q", std::string(sql));
            client_->put_node_async(local_cluster_id_, sid, buf.move_to_string()).get();
            return SUCCESS();
        }
        irods::error delete_specific_query(std::string_view alias) {
            snowflake_id_t sid = SnowflakeID::create(local_cluster_id_, "sq:" + std::string(alias));
            client_->del_node_async(local_cluster_id_, sid).get();
            return SUCCESS();
        }

        // --- Logical Quota ---
        irods::error set_logical_quota(std::string_view coll_name, int64_t limit) {
            snowflake_id_t cid = resolve_id_from_index(EntityType::Collection, "n", coll_name);
            if (!cid) return ERROR(-1, "Collection not found");

            std::string q_uuid = "lquota:" + std::string(coll_name);
            snowflake_id_t qid = SnowflakeID::create(local_cluster_id_, q_uuid);

            lite3cpp::Buffer buf; buf.init_object(); buf.set_i64(0, "limit", limit);
            client_->put_node_async(local_cluster_id_, qid, buf.move_to_string()).get();

            add_edge(cid, "HAS_LOGICAL_QUOTA", 1.0, qid);
            return SUCCESS();
        }
        irods::error check_logical_quota(std::string_view coll_name, int64_t& usage, int64_t& limit) {
            usage = 0; limit = -1;
            snowflake_id_t cid = resolve_id_from_index(EntityType::Collection, "n", coll_name);
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
            usage = 0;
            snowflake_id_t cid = resolve_id_from_index(EntityType::Collection, "n", coll_name);
            if (!cid) return SUCCESS();

            // Recursive traversal to sum all DataObjects
            std::vector<snowflake_id_t> stack = {cid};
            while (!stack.empty()) {
                snowflake_id_t current = stack.back(); stack.pop_back();
                
                auto contents = client_->get_neighbors_async(local_cluster_id_, current, "CONTAINS", 0.0).get();
                for (auto id : contents) {
                    std::string payload = client_->get_node_payload_async(local_cluster_id_, id).get();
                    if (!payload.empty()) {
                        try {
                            lite3cpp::Buffer buf(std::vector<uint8_t>(payload.begin(), payload.end()));
                            if (buf.get_type(0, "s") != lite3cpp::Type::Invalid) {
                                usage += buf.get_i64(0, "s");
                            } else {
                                // It's a collection, add to stack for recursive summation
                                stack.push_back(id);
                            }
                        } catch (...) {}
                    }
                }
            }
            return SUCCESS();
        }

        // --- Server Load ---
        irods::error register_server_load(std::string_view host, int load) {
            snowflake_id_t sid = SnowflakeID::create(local_cluster_id_, "load:" + std::string(host));
            lite3cpp::Buffer buf; buf.init_object(); buf.set_str(0, "h", std::string(host)); buf.set_i64(0, "l", load);
            client_->put_node_async(local_cluster_id_, sid, buf.move_to_string()).get();
            snowflake_id_t zid = make_id(EntityType::Zone, 1);
            add_edge(zid, "HAS_LOAD", 1.0, sid);
            return SUCCESS();
        }
        irods::error purge_server_load(std::string_view host) {
            snowflake_id_t sid = SnowflakeID::create(local_cluster_id_, "load:" + std::string(host));
            client_->del_node_async(local_cluster_id_, sid).get();
            return SUCCESS();
        }

        // --- Grid Config ---
        irods::error set_grid_configuration_value(std::string_view key, std::string_view value) {
            snowflake_id_t sid = SnowflakeID::create(local_cluster_id_, "grid:" + std::string(key));
            lite3cpp::Buffer buf; buf.init_object(); buf.set_str(0, "v", std::string(value));
            client_->put_node_async(local_cluster_id_, sid, buf.move_to_string()).get();
            return SUCCESS();
        }
        irods::error get_grid_configuration_value(std::string_view key, std::string& out_value) {
            snowflake_id_t sid = SnowflakeID::create(local_cluster_id_, "grid:" + std::string(key));
            std::string payload = client_->get_node_payload_async(local_cluster_id_, sid).get();
            if (!payload.empty()) {
                try {
                    lite3cpp::Buffer buf(std::vector<uint8_t>(payload.begin(), payload.end()));
                    out_value = safe_get_str(buf, 0, "v");
                } catch (...) { return ERROR(-1, "Failed to parse grid config"); }
            }
            return SUCCESS();
        }

        // --- Query ---
        irods::error resolve_path(std::string_view path, snowflake_id_t& out_id, EntityType& out_type) {
            snowflake_id_t sid = resolve_id_from_index(EntityType::Collection, "n", path);
            if (sid) { out_id = sid; out_type = EntityType::Collection; return SUCCESS(); }
            
            sid = resolve_id_from_index(EntityType::DataObject, "path", path);
            if (sid) { out_id = sid; out_type = EntityType::DataObject; return SUCCESS(); }

            sid = resolve_id_from_index(EntityType::DataObject, "n", path);
            if (sid) { out_id = sid; out_type = EntityType::DataObject; return SUCCESS(); }

            return ERROR(-1, "Path not found: " + std::string(path));
        }

        irods::error execute_query(const irods::experimental::genquery2::select& ast, ResultSet& results, const std::vector<uint64_t>& starting_nodes = {}, std::string_view root_type = "") {
            try {
                std::string effective_root_type(root_type);
                std::vector<uint64_t> sn = starting_nodes;
                if (!sn.empty() && sn[0] == 0) sn.clear();


                compiler::Gq2ToL3kvgCompiler compiler;
                std::string query_json = compiler.compile(ast, effective_root_type);
                if (effective_root_type.empty()) {
                    effective_root_type = compiler.get_entry_type();
                }

                if (sn.empty() && effective_root_type == "Zone") {
                    snowflake_id_t zid = make_id(EntityType::Zone, 1);
                    sn.push_back(zid);
                } else if (sn.empty() && effective_root_type == "Resource") {
                    snowflake_id_t zid = make_id(EntityType::Zone, 1);
                    auto resc_nodes = client_->get_neighbors_async(local_cluster_id_, zid, "HAS_RESC", 0.0).get();
                    sn = std::move(resc_nodes);
                } else if (sn.empty() && effective_root_type == "User") {
                    snowflake_id_t zid = make_id(EntityType::Zone, 1);
                    auto user_nodes = client_->get_neighbors_async(local_cluster_id_, zid, "HAS_USER", 0.0).get();
                    sn = std::move(user_nodes);
                }

            #ifdef IRODS_SERVER
            rodsLog(LOG_NOTICE, "L3_CATALOG: Executing Query with root_type [%s] and [%zu] starting nodes: %s", effective_root_type.c_str(), sn.size(), query_json.c_str());
            #endif

                auto fut = client_->resume_query_async(local_cluster_id_, sn, query_json);
                results.rows = fut.get();

            #ifdef IRODS_SERVER
            rodsLog(LOG_NOTICE, "L3_CATALOG: Query returned %zu rows", results.rows.size());
            for (size_t r = 0; r < std::min(results.rows.size(), size_t(5)); ++r) {
                for (const auto& [k, v] : results.rows[r].fields) {
                    rodsLog(LOG_NOTICE, "L3_CATALOG: Result Row %zu: [%s]=[%s]", r, k.c_str(), v.c_str());
                }
            }
            #endif

                return SUCCESS();
            } catch (const std::exception& e) {
            #ifdef IRODS_SERVER
                rodsLog(LOG_ERROR, "L3KVG: execute_query exception: %s", e.what());
            #endif
                return ERROR(-1, e.what());
            } catch (...) {
            #ifdef IRODS_SERVER
                rodsLog(LOG_ERROR, "L3KVG: execute_query unknown exception");
            #endif
                return ERROR(-1, "Unknown exception in execute_query");
            }
        }

        irods::error apply_atomic_operations(const std::vector<irods::experimental::dml::operation_type>& ops) {
            // Stub for atomic operations implementation
            return SUCCESS();
        }

        irods::error execute_dml(const compiler::DmlPlan& plan, nlohmann::json& result) {
            try {
                auto get_entity_type = [](std::string_view name, EntityType& out) -> bool {
                    if (name == "DataObject" || name == "DATA_NAME" || name == "DATA" || name == "data_object") {
                        out = EntityType::DataObject; return true;
                    }
                    if (name == "Collection" || name == "COLLECTION" || name == "COLL" || name == "collection") {
                        out = EntityType::Collection; return true;
                    }
                    if (name == "User" || name == "USER" || name == "Group" || name == "GROUP" || name == "user") {
                        out = EntityType::User; return true;
                    }
                    if (name == "Resource" || name == "RESOURCE" || name == "RESC" || name == "resource") {
                        out = EntityType::Resource; return true;
                    }
                    if (name == "Zone" || name == "ZONE" || name == "zone") {
                        out = EntityType::Zone; return true;
                    }
                    if (name == "Replica" || name == "REPLICA" || name == "replica") {
                        out = EntityType::Replica; return true;
                    }
                    if (name == "Rule" || name == "RULE" || name == "rule") {
                        out = EntityType::Rule; return true;
                    }
                    if (name == "Metadata" || name == "METADATA" || name == "metadata") {
                        out = EntityType::Metadata; return true;
                    }
                    if (name == "Ticket" || name == "TICKET" || name == "ticket") {
                        out = EntityType::Ticket; return true;
                    }
                    return false;
                };

                auto get_seq_name = [](EntityType et) -> std::string {
                    switch (et) {
                        case EntityType::Collection: return "R_COLL_MAIN";
                        case EntityType::User: return "R_USER_MAIN";
                        case EntityType::Resource: return "R_RESC_MAIN";
                        case EntityType::Zone: return "R_ZONE_MAIN";
                        case EntityType::Rule: return "R_RULE_EXEC";
                        default: return "R_DATA_MAIN";
                    }
                };

                EntityType et;
                if (!get_entity_type(plan.entity_type, et)) {
                    return ERROR(SYS_INVALID_INPUT_PARAM, "Unknown or unsupported entity type: " + plan.entity_type);
                }

                auto get_prop_val = [&](lite3cpp::Buffer& b, const std::string& prop) -> std::string {
                    if (prop == "id") {
                        return safe_get_str(b, 0, "id");
                    }
                    if (prop == "s" || prop == "size" || prop == "DATA_SIZE") {
                        return safe_get_str(b, 0, "s");
                    }
                    if (prop == "n" || prop == "name" || prop == "DATA_NAME" || prop == "COLL_NAME") {
                        return safe_get_str(b, 0, "n");
                    }
                    if (prop == "p" || prop == "path") {
                        return safe_get_str(b, 0, "p");
                    }
                    if (prop == "COLL_PARENT_NAME" || prop == "parent_coll" || prop == "parent_collection" || prop == "pn") {
                        std::string pn = safe_get_str(b, 0, "pn");
                        if (!pn.empty()) return pn;
                        return safe_get_str(b, 0, "p");
                    }
                    if (prop == "o" || prop == "owner" || prop == "owner_name" || prop == "COLL_OWNER_NAME" || prop == "DATA_OWNER_NAME") {
                        return safe_get_str(b, 0, "o");
                    }
                    if (prop == "ex" || prop == "expiry" || prop == "DATA_EXPIRY") {
                        std::string ex = safe_get_str(b, 0, "ex");
                        return ex.empty() ? "00000000000" : ex;
                    }
                    return safe_get_str(b, 0, prop);
                };

                auto compare_vals = [](const std::string& actual, int op, const std::string& expected) -> bool {
                    bool is_num = false;
                    int64_t a_num = 0, e_num = 0;
                    try {
                        size_t a_pos = 0, e_pos = 0;
                        if (!actual.empty() && !expected.empty()) {
                            a_num = std::stoll(actual, &a_pos);
                            e_num = std::stoll(expected, &e_pos);
                            if (a_pos == actual.size() && e_pos == expected.size()) {
                                is_num = true;
                            }
                        }
                    } catch (...) {}

                    if (is_num) {
                        switch (op) {
                            case 0: return a_num == e_num;
                            case 1: return a_num != e_num;
                            case 2: return a_num > e_num;
                            case 3: return a_num >= e_num;
                            case 4: return a_num < e_num;
                            case 5: return a_num <= e_num;
                            default: return a_num == e_num;
                        }
                    } else {
                        switch (op) {
                            case 0: return actual == expected;
                            case 1: return actual != expected;
                            case 2: return actual > expected;
                            case 3: return actual >= expected;
                            case 4: return actual < expected;
                            case 5: return actual <= expected;
                            default: return actual == expected;
                        }
                    }
                };

                auto resolve_target_sid = [&](EntityType target_et, const std::vector<compiler::DmlCondition>& conditions) -> snowflake_id_t {
                    // 1. Specificity: check ID
                    for (const auto& cond : conditions) {
                        if (cond.op == 0 && cond.property == "id") {
                            try {
                                snowflake_id_t s = make_id(target_et, std::stoull(cond.value));
                                if (s) return s;
                            } catch (...) {}
                        }
                    }
                    // 2. Specificity: check path / p
                    for (const auto& cond : conditions) {
                        if (cond.op == 0 && (cond.property == "path" || cond.property == "p" || cond.property == "COLL_NAME")) {
                            snowflake_id_t s = resolve_id_from_index(target_et, "path", cond.value);
                            if (s) return s;
                        }
                    }
                    // 3. Specificity: check n / name
                    for (const auto& cond : conditions) {
                        if (cond.op == 0 && (cond.property == "n" || cond.property == "name" || cond.property == "DATA_NAME")) {
                            snowflake_id_t s = resolve_id_from_index(target_et, "n", cond.value);
                            if (s) return s;
                        }
                    }
                    // 4. Other equality conditions
                    for (const auto& cond : conditions) {
                        if (cond.op == 0) {
                            snowflake_id_t s = resolve_id_from_index(target_et, cond.property, cond.value);
                            if (s) return s;
                        }
                    }
                    return 0;
                };

                if (plan.action == compiler::DmlAction::Insert) {
                    uint64_t irods_id = 0;
                    auto id_it = plan.properties.find("id");
                    if (id_it != plan.properties.end()) {
                        try {
                            irods_id = std::stoull(id_it->second);
                        } catch (...) {}
                    }

                    if (irods_id == 0) {
                        std::string seq_name = get_seq_name(et);
                        auto seq_err = get_next_sequence_value(seq_name, irods_id);
                        if (!seq_err.ok()) {
                            return seq_err;
                        }
                    }

                    snowflake_id_t sid = make_id(et, irods_id);

                    std::string name;
                    auto n_it = plan.properties.find("n");
                    if (n_it == plan.properties.end()) n_it = plan.properties.find("name");
                    if (n_it != plan.properties.end()) name = n_it->second;

                    std::string parent_coll;
                    auto pc_it = plan.properties.find("parent_coll");
                    if (pc_it != plan.properties.end()) {
                        parent_coll = pc_it->second;
                    } else {
                        auto pn_it = plan.properties.find("parent_collection");
                        if (pn_it != plan.properties.end()) {
                            parent_coll = pn_it->second;
                        } else {
                            auto pnc_it = plan.properties.find("pn");
                            if (pnc_it != plan.properties.end()) {
                                parent_coll = pnc_it->second;
                            }
                        }
                    }

                    std::string explicit_path;
                    auto p_it = plan.properties.find("path");
                    if (p_it != plan.properties.end()) {
                        explicit_path = p_it->second;
                    } else {
                        auto p2_it = plan.properties.find("p");
                        if (p2_it != plan.properties.end()) {
                            explicit_path = p2_it->second;
                        }
                    }

                    std::string full_path = explicit_path;
                    if (full_path.empty() && !parent_coll.empty() && !name.empty()) {
                        if (name.front() == '/') {
                            full_path = name;
                        } else {
                            full_path = parent_coll;
                            if (full_path.empty() || full_path.back() != '/') {
                                full_path += "/";
                            }
                            full_path += name;
                        }
                    }

                    std::string owner_name;
                    auto o_it = plan.properties.find("o");
                    if (o_it == plan.properties.end()) o_it = plan.properties.find("owner");
                    if (o_it == plan.properties.end()) o_it = plan.properties.find("owner_name");
                    if (o_it != plan.properties.end()) owner_name = o_it->second;

                    lite3cpp::Buffer buf;
                    buf.init_object();
                    buf.set_i64(0, "id", static_cast<int64_t>(irods_id));

                    for (const auto& [k, v] : plan.properties) {
                        if (k == "id") continue;
                        if (k == "parent_coll" || k == "parent_collection" || k == "pn") continue;
                        if (k == "path" || k == "p") continue;
                        if (k == "name") {
                            buf.set_str(0, "n", v);
                            continue;
                        }
                        if (k == "owner" || k == "owner_name") {
                            buf.set_str(0, "o", v);
                            continue;
                        }
                        if (k == "s" || k == "rid" || k == "rn" || k == "size" || k == "DATA_SIZE") {
                            try {
                                buf.set_i64(0, (k == "size" || k == "DATA_SIZE" ? "s" : k), std::stoll(v));
                                continue;
                            } catch (...) {}
                        }
                        buf.set_str(0, k, v);
                    }

                    if (!full_path.empty()) {
                        buf.set_str(0, "p", full_path);
                    }

                    client_->put_node_async(local_cluster_id_, sid, buf.move_to_string()).get();

                    if (!name.empty()) {
                        add_index(et, "n", name, sid);
                    }

                    if (!full_path.empty()) {
                        add_index(et, "path", full_path, sid);
                    }

                    if (!parent_coll.empty()) {
                        snowflake_id_t parent_sid = resolve_id_from_index(EntityType::Collection, "n", parent_coll);
                        if (!parent_sid) {
                            parent_sid = resolve_id_from_index(EntityType::Collection, "path", parent_coll);
                        }
                        if (!parent_sid) {
                            try {
                                parent_sid = make_id(EntityType::Collection, std::stoull(parent_coll));
                            } catch (...) {}
                        }
                        if (parent_sid) {
                            add_edge(parent_sid, "CONTAINS", 1.0, sid);
                        }
                    }

                    if (!owner_name.empty()) {
                        snowflake_id_t user_sid = resolve_id_from_index(EntityType::User, "n", owner_name);
                        if (user_sid) {
                            add_edge(user_sid, "OWNS", 1.0, sid);
                        }
                    }

                    result["rows_affected"] = 1;
                    result["status"] = "SUCCESS";
                    return SUCCESS();
                } else if (plan.action == compiler::DmlAction::Update) {
                    snowflake_id_t sid = resolve_target_sid(et, plan.conditions);

                    if (sid == 0) {
                        result["rows_affected"] = 0;
                        result["status"] = "SUCCESS";
                        return SUCCESS();
                    }

                    std::string payload = client_->get_node_payload_async(local_cluster_id_, sid).get();
                    if (payload.empty()) {
                        result["rows_affected"] = 0;
                        result["status"] = "SUCCESS";
                        return SUCCESS();
                    }

                    lite3cpp::Buffer buf(std::vector<uint8_t>(payload.begin(), payload.end()));

                    // Secondary filter verification against buf
                    for (const auto& cond : plan.conditions) {
                        std::string actual = get_prop_val(buf, cond.property);
                        if (!compare_vals(actual, cond.op, cond.value)) {
                            result["rows_affected"] = 0;
                            result["status"] = "SUCCESS";
                            return SUCCESS();
                        }
                    }

                    std::string old_n = safe_get_str(buf, 0, "n");
                    std::string old_p = safe_get_str(buf, 0, "p");

                    std::string new_n;
                    std::string new_p;

                    for (const auto& [k, v] : plan.properties) {
                        if (k == "n" || k == "name") {
                            new_n = v;
                            buf.set_str(0, "n", v);
                            continue;
                        }
                        if (k == "path" || k == "p") {
                            new_p = v;
                            buf.set_str(0, "p", v);
                            continue;
                        }
                        if (k == "owner" || k == "owner_name") {
                            buf.set_str(0, "o", v);
                            continue;
                        }
                        if (k == "s" || k == "rid" || k == "rn" || k == "size" || k == "DATA_SIZE" || k == "id") {
                            try {
                                buf.set_i64(0, (k == "size" || k == "DATA_SIZE" ? "s" : k), std::stoll(v));
                                continue;
                            } catch (...) {}
                        }
                        buf.set_str(0, k, v);
                    }

                    // Recompute path index if basename changed without explicit path
                    if (!new_n.empty() && new_n != old_n && new_p.empty() && !old_p.empty()) {
                        auto last_slash = old_p.find_last_of('/');
                        if (last_slash != std::string::npos) {
                            new_p = old_p.substr(0, last_slash + 1) + new_n;
                            buf.set_str(0, "p", new_p);
                        }
                    }

                    if (!new_n.empty() && new_n != old_n) {
                        if (!old_n.empty()) del_index(et, "n", old_n);
                        add_index(et, "n", new_n, sid);
                    }

                    if (!new_p.empty() && new_p != old_p) {
                        if (!old_p.empty()) del_index(et, "path", old_p);
                        add_index(et, "path", new_p, sid);
                    }

                    client_->put_node_async(local_cluster_id_, sid, buf.move_to_string()).get();

                    result["rows_affected"] = 1;
                    result["status"] = "SUCCESS";
                    return SUCCESS();
                } else if (plan.action == compiler::DmlAction::Remove) {
                    snowflake_id_t sid = resolve_target_sid(et, plan.conditions);

                    if (sid == 0) {
                        result["rows_affected"] = 0;
                        result["status"] = "SUCCESS";
                        return SUCCESS();
                    }

                    std::string payload = client_->get_node_payload_async(local_cluster_id_, sid).get();
                    if (payload.empty()) {
                        result["rows_affected"] = 0;
                        result["status"] = "SUCCESS";
                        return SUCCESS();
                    }

                    lite3cpp::Buffer buf(std::vector<uint8_t>(payload.begin(), payload.end()));

                    // Secondary filter verification against buf
                    for (const auto& cond : plan.conditions) {
                        std::string actual = get_prop_val(buf, cond.property);
                        if (!compare_vals(actual, cond.op, cond.value)) {
                            result["rows_affected"] = 0;
                            result["status"] = "SUCCESS";
                            return SUCCESS();
                        }
                    }

                    // 1. Delete outgoing HAS_REPLICA edges and child replica nodes if DataObject
                    if (et == EntityType::DataObject) {
                        try {
                            auto replicas = client_->get_neighbors_async(local_cluster_id_, sid, "HAS_REPLICA", 0.0).get();
                            for (auto rid : replicas) {
                                client_->del_node_async(local_cluster_id_, rid);
                            }
                        } catch (...) {}
                    }

                    // 2. Clean up indices extracted directly from buf
                    std::string name = safe_get_str(buf, 0, "n");
                    if (!name.empty()) del_index(et, "n", name);
                    std::string path = safe_get_str(buf, 0, "p");
                    if (!path.empty()) del_index(et, "path", path);

                    // 3. Delete incoming edges (CONTAINS, OWNS)
                    try {
                        auto collections = client_->get_in_neighbors_async(local_cluster_id_, sid, "CONTAINS").get();
                        for (auto cid : collections) {
                            del_edge(cid, "CONTAINS", 1.0, sid);
                        }
                        auto owners = client_->get_in_neighbors_async(local_cluster_id_, sid, "OWNS").get();
                        for (auto oid : owners) {
                            del_edge(oid, "OWNS", 1.0, sid);
                        }
                    } catch (...) {}

                    // 4. Delete the node itself
                    client_->del_node_async(local_cluster_id_, sid).get();

                    result["rows_affected"] = 1;
                    result["status"] = "SUCCESS";
                    return SUCCESS();
                }

                return ERROR(-1, "Unsupported DML action");
            } catch (const std::exception& e) {
                return ERROR(-1, e.what());
            } catch (...) {
                return ERROR(-1, "Unknown exception in execute_dml");
            }
        }

    private:
        std::unique_ptr<l3kvg::RemoteL3KVClient> client_;
        uint16_t local_cluster_id_ = 0;
        std::string local_zone_name_;
    };

    CatalogFacade::CatalogFacade() : pImpl_(std::make_unique<CatalogImpl>()) {}
    CatalogFacade::~CatalogFacade() = default;
    irods::error CatalogFacade::init(const Config& cfg, std::string_view zone_name, const l3kvg::Settings& settings) { return pImpl_->init(cfg, zone_name, settings); }
    irods::error CatalogFacade::bootstrap_catalog(std::string_view zone_name, std::string_view admin_name) { return pImpl_->bootstrap_catalog(zone_name, admin_name); }
    irods::error CatalogFacade::bootstrap_federation(const std::vector<FederatedZone>& peers) { return pImpl_->bootstrap_federation(peers); }
    irods::error CatalogFacade::register_data_object(const data_object& obj, data_id_t& out_id) { return pImpl_->register_data_object(obj, out_id); }
    irods::error CatalogFacade::delete_data_object(data_id_t id) { return pImpl_->delete_data_object(id); }
    irods::error CatalogFacade::rename_data_object(data_id_t obj_id, std::string_view new_name) { return pImpl_->rename_data_object(obj_id, new_name); }
    irods::error CatalogFacade::move_data_object(data_id_t obj_id, coll_id_t target_coll_id) { return pImpl_->move_data_object(obj_id, target_coll_id); }
    irods::error CatalogFacade::modify_data_object(data_id_t obj_id, std::string_view prop, std::string_view value) { return pImpl_->modify_data_object(obj_id, prop, value); }
    irods::error CatalogFacade::rename_object(uint64_t obj_id, std::string_view new_name) { return pImpl_->rename_object(obj_id, new_name); }
    irods::error CatalogFacade::move_object(uint64_t obj_id, coll_id_t target_coll_id) { return pImpl_->move_object(obj_id, target_coll_id); }
    irods::error CatalogFacade::register_replica(const replica& repl) { return pImpl_->register_replica(repl); }
    irods::error CatalogFacade::unregister_replica(data_id_t data_id, uint32_t repl_num) { return pImpl_->unregister_replica(data_id, repl_num); }
    irods::error CatalogFacade::update_replica_access_time(data_id_t data_id, uint32_t repl_num, std::string_view time) { return pImpl_->update_replica_access_time(data_id, repl_num, time); }
    irods::error CatalogFacade::modify_replicas_for_data_object(data_id_t obj_id, uint32_t repl_num, const std::vector<std::pair<std::string, std::string>>& updates, bool all_repl_status) { return pImpl_->modify_replicas_for_data_object(obj_id, repl_num, updates, all_repl_status); }
    uint32_t CatalogFacade::get_next_replica_number(data_id_t data_id) { return pImpl_->get_next_replica_number(data_id); }
    irods::error CatalogFacade::register_collection(const collection& coll, coll_id_t& out_id) { return pImpl_->register_collection(coll, out_id); }
    irods::error CatalogFacade::rename_collection(std::string_view old_name, std::string_view new_name) { return pImpl_->rename_collection(old_name, new_name); }
    irods::error CatalogFacade::delete_collection(coll_id_t coll_id) { return pImpl_->delete_collection(coll_id); }
    irods::error CatalogFacade::modify_collection(coll_id_t coll_id, std::string_view prop, std::string_view value) { return pImpl_->modify_collection(coll_id, prop, value); }
    irods::error CatalogFacade::register_resource(const resource& resc, resc_id_t& out_id) { return pImpl_->register_resource(resc, out_id); }
    irods::error CatalogFacade::modify_resource(snowflake_id_t sid, std::string_view prop, std::string_view value) { return pImpl_->modify_resource(sid, prop, value); }
    irods::error CatalogFacade::delete_resource(snowflake_id_t sid) { return pImpl_->delete_resource(sid); }
    irods::error CatalogFacade::resolve_resource_name(std::string_view name, snowflake_id_t& out_id) { return pImpl_->resolve_resource_name(name, out_id); }
    irods::error CatalogFacade::resolve_user_name(std::string_view name, snowflake_id_t& out_id) { return pImpl_->resolve_user_name(name, out_id); }
    irods::error CatalogFacade::get_hierarchy_for_resource(std::string_view name, std::string& out_hier) { return pImpl_->get_hierarchy_for_resource(name, out_hier); }
    irods::error CatalogFacade::update_resource_object_count(resc_id_t resc_id, int delta) { return pImpl_->update_resource_object_count(resc_id, delta); }
    irods::error CatalogFacade::add_child_resource(std::string_view parent_name, std::string_view child_name, std::string_view context) { return pImpl_->add_child_resource(parent_name, child_name, context); }
    irods::error CatalogFacade::remove_child_resource(std::string_view parent_name, std::string_view child_name) { return pImpl_->remove_child_resource(parent_name, child_name); }
    irods::error CatalogFacade::register_user(const user& usr, user_id_t& out_id) { return pImpl_->register_user(usr, out_id); }
    irods::error CatalogFacade::delete_user(std::string_view user_name) { return pImpl_->delete_user(user_name); }
    irods::error CatalogFacade::modify_user(std::string_view user_name, std::string_view prop, std::string_view value) { return pImpl_->modify_user(user_name, prop, value); }
    irods::error CatalogFacade::check_auth(std::string_view user_name, std::string_view zone, int& user_priv) { return pImpl_->check_auth(user_name, zone, user_priv); }
    irods::error CatalogFacade::get_user_password_and_priv(std::string_view user_name, std::string_view zone, std::string& out_pw, int& out_priv) { return pImpl_->get_user_password_and_priv(user_name, zone, out_pw, out_priv); }
    irods::error CatalogFacade::check_auth_credentials(std::string_view username, std::string_view zone, std::string_view password, bool& correct) { return pImpl_->check_auth_credentials(username, zone, password, correct); }
    irods::error CatalogFacade::add_user_to_group(std::string_view user_name, std::string_view zone, std::string_view group_name) { return pImpl_->add_user_to_group(user_name, zone, group_name); }
    irods::error CatalogFacade::remove_user_from_group(std::string_view user_name, std::string_view zone, std::string_view group_name) { return pImpl_->remove_user_from_group(user_name, zone, group_name); }
    irods::error CatalogFacade::set_access(std::string_view user_name, std::string_view zone, std::string_view target_path, std::string_view level, bool recursive) { return pImpl_->set_access(user_name, zone, target_path, level, recursive); }
    irods::error CatalogFacade::check_permission(snowflake_id_t user_id, snowflake_id_t target_id, std::string_view level, bool& allowed) { return pImpl_->check_permission(user_id, target_id, level, allowed); }
    irods::error CatalogFacade::check_permission_to_modify_data_object(snowflake_id_t user_id, snowflake_id_t target_id, bool& allowed) { return pImpl_->check_permission_to_modify_data_object(user_id, target_id, allowed); }

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

    irods::error CatalogFacade::resolve_path(std::string_view path, snowflake_id_t& out_id, EntityType& out_type) { return pImpl_->resolve_path(path, out_id, out_type); }
    irods::error CatalogFacade::get_collection_subtree_ids(snowflake_id_t coll_sid, std::vector<snowflake_id_t>& out_ids) { return pImpl_->get_collection_subtree_ids(coll_sid, out_ids); }
    irods::error CatalogFacade::execute_query(const irods::experimental::genquery2::select& ast, ResultSet& results, const std::vector<uint64_t>& starting_nodes, std::string_view root_type) { return pImpl_->execute_query(ast, results, starting_nodes, root_type); }
    irods::error CatalogFacade::execute_dml(const compiler::DmlPlan& plan, nlohmann::json& result) { return pImpl_->execute_dml(plan, result); }
    irods::error CatalogFacade::apply_atomic_operations(const std::vector<irods::experimental::dml::operation_type>& ops) { return pImpl_->apply_atomic_operations(ops); }
    irods::error CatalogFacade::get_next_sequence_value(std::string_view seq_name, uint64_t& out_val) { return pImpl_->get_next_sequence_value(seq_name, out_val); }
    snowflake_id_t CatalogFacade::make_id(EntityType type, uint64_t irods_id) { return pImpl_->make_id(type, irods_id); }
    snowflake_id_t CatalogFacade::resolve_id_from_index(EntityType type, std::string_view attr, std::string_view value) { return pImpl_->resolve_id_from_index(type, attr, value); }

    l3kvg::RemoteL3KVClient* CatalogFacade::get_client() const { return pImpl_->get_client(); }
    uint16_t CatalogFacade::get_cluster_id() const { return pImpl_->get_cluster_id(); }

} // namespace irods::catalog
