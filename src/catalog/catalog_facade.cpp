#include "irods/catalog/catalog_facade.hpp"
#include "irods/catalog/binary_key.hpp"
#include "irods/catalog/federation_resolver.hpp"
#include "L3KVG/RemoteL3KVClient.hpp"
#include "L3KVG/Node.hpp"
#include "L3KVG/KeyBuilder.hpp"
#include "L3KVG/MutationBatch.hpp"
#include "irods/catalog/l3kvg_mapper.hpp"
#include "irods/catalog/catalog_schemas.hpp"
#include "irods/catalog/gq2_compiler.hpp"
#include "irods/filesystem/path.hpp"
#include "irods/rodsErrorTable.h"
#include "irods/irods_children_parser.hpp"
#include "irods/rcMisc.h"
#include <iostream>
#include <cstdio>
#include <random>
#include <algorithm>
#include <unordered_set>
#include <set>
#include <tuple>
#include <ctime>
#include <chrono>
#include <netdb.h>
#include <arpa/inet.h>
#include <sys/socket.h>

#ifdef IRODS_SERVER
#include "irods/rodsLog.h"
#define CAT_LOG(level, ...) rodsLog(level, __VA_ARGS__)
#else
#define CAT_LOG(level, ...) 
#endif

namespace irods::catalog {

    struct UserInfoCache {
        std::string name;
        std::string type;
        std::vector<snowflake_id_t> principals;
        std::chrono::steady_clock::time_point expires_at;
    };
    static std::mutex s_user_cache_mu;
    static std::unordered_map<snowflake_id_t, UserInfoCache> s_user_cache;

    static std::mutex s_reg_cache_mu;
    static std::unordered_map<snowflake_id_t, std::string> s_coll_path_cache;
    static std::unordered_map<std::string, snowflake_id_t> s_user_id_cache;
    static std::mutex s_path_cache_mu;
    static std::unordered_map<std::string, snowflake_id_t> s_coll_name_cache;
    struct UserMembersCacheEntry {
        std::vector<snowflake_id_t> members;
        std::chrono::steady_clock::time_point expires_at;
    };
    static std::mutex s_user_members_mu;
    static std::unordered_map<snowflake_id_t, UserMembersCacheEntry> s_user_members_cache;

    static void invalidate_user_cache(snowflake_id_t uid) {
        if (uid != 0) {
            std::lock_guard<std::mutex> lock(s_user_cache_mu);
            s_user_cache.erase(uid);
        }
        {
            std::lock_guard<std::mutex> lock(s_reg_cache_mu);
            s_user_id_cache.clear();
        }
        {
            std::lock_guard<std::mutex> lock(s_user_members_mu);
            if (uid != 0) {
                s_user_members_cache.erase(uid);
            } else {
                s_user_members_cache.clear();
            }
        }
    }

    class CatalogImpl {
    public:
        CatalogImpl() : objid_curr_{0}, objid_limit_{0} {}

        irods::error init(const Config& cfg, std::string_view zone_name, const l3kvg::Settings& settings) {
            try {
                local_zone_name_ = std::string(zone_name);
                client_ = std::make_unique<l3kvg::RemoteL3KVClient>(settings);
                
                auto pool = std::make_shared<l3kvg::ThreadPool>(std::clamp(std::thread::hardware_concurrency(), 2u, 8u));
                client_->set_thread_pool(pool);

                local_cluster_id_ = cfg.cluster_id; 
                client_->add_peer(cfg.node_id, cfg.zmq_endpoint);
                client_->add_peer(local_cluster_id_, cfg.zmq_endpoint);
                for (const auto& fed : cfg.federation) { client_->add_peer(fed.id, fed.endpoint); }
                if (!cfg.federation.empty()) {
                    bootstrap_federation(cfg.federation);
                }

                snowflake_id_t zid = resolve_id_from_index(EntityType::Zone, "n", zone_name);
                if (!zid) {
                    zid = make_id(EntityType::Zone, 1);
                }
                std::string zpayload = client_->get_node_payload_async(local_cluster_id_, zid).get();
                if (zpayload.empty()) {
                    char time_buf[32];
                    snprintf(time_buf, sizeof(time_buf), "%011lld", (long long)time(nullptr));
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
                    add_edge(zid, "HAS_ZONE", 1.0, zid);

                    std::string adm_user = cfg.admin_user.empty() ? "rods" : cfg.admin_user;
                    snowflake_id_t uid = resolve_id_from_index(EntityType::User, "nz", adm_user + "#" + std::string(zone_name));
                    if (!uid) {
                        uid = resolve_id_from_index(EntityType::User, "n", adm_user);
                    }
                    if (!uid) {
                        uid = make_id(EntityType::User, 1);
                    }
                    std::string upayload = client_->get_node_payload_async(local_cluster_id_, uid).get();
                    if (upayload.empty()) {
                        lite3cpp::Buffer ubuf; ubuf.init_object(); 
                        ubuf.set_str(0, "n", adm_user); 
                        ubuf.set_str(0, "t", "rodsadmin"); 
                        ubuf.set_str(0, "z", std::string(zone_name));
                        ubuf.set_i64(0, "p", 5); 
                        ubuf.set_str(0, "pw", adm_user);
                        ubuf.set_str(0, "c", "");
                        ubuf.set_str(0, "i", "");
                        ubuf.set_str(0, "ct", time_buf);
                        ubuf.set_str(0, "mt", time_buf);
                        ubuf.set_i64(0, "id", 1);
                        client_->put_node_async(local_cluster_id_, uid, ubuf.move_to_string()).get();
                    } else {
                        try {
                            lite3cpp::Buffer ubuf(upayload);
                            if (safe_get_str(ubuf, 0, "ct").empty()) {
                                ubuf.set_str(0, "ct", time_buf);
                                ubuf.set_str(0, "mt", time_buf);
                                ubuf.set_str(0, "c", "");
                                ubuf.set_str(0, "i", "");
                                client_->put_node_async(local_cluster_id_, uid, ubuf.move_to_string()).get();
                            }
                        } catch (...) {}
                    }
                    add_index(EntityType::User, "nz", adm_user + "#" + std::string(zone_name), uid);
                    add_index(EntityType::User, "n", adm_user, uid);
                    add_index(EntityType::User, "id", "1", uid);
                    add_edge(zid, "HAS_USER", 1.0, uid);
                    add_edge(uid, "MEMBER_OF", 1.0, uid);

                    snowflake_id_t gid_pub = resolve_id_from_index(EntityType::User, "nz", "public#" + std::string(zone_name));
                    if (!gid_pub) {
                        gid_pub = resolve_id_from_index(EntityType::User, "n", "public");
                    }
                    if (!gid_pub) {
                        gid_pub = make_id(EntityType::User, 2);
                    }
                    std::string pub_payload = client_->get_node_payload_async(local_cluster_id_, gid_pub).get();
                    if (pub_payload.empty()) {
                        lite3cpp::Buffer gbuf_public; gbuf_public.init_object(); 
                        gbuf_public.set_str(0, "n", "public"); 
                        gbuf_public.set_str(0, "t", "rodsgroup"); 
                        gbuf_public.set_str(0, "z", std::string(zone_name));
                        gbuf_public.set_str(0, "c", "");
                        gbuf_public.set_str(0, "i", "");
                        gbuf_public.set_str(0, "ct", time_buf);
                        gbuf_public.set_str(0, "mt", time_buf);
                        gbuf_public.set_i64(0, "id", 2);
                        client_->put_node_async(local_cluster_id_, gid_pub, gbuf_public.move_to_string()).get();
                    }
                    add_index(EntityType::User, "nz", "public#" + std::string(zone_name), gid_pub);
                    add_index(EntityType::User, "n", "public", gid_pub);
                    add_index(EntityType::User, "id", "2", gid_pub);
                    add_edge(zid, "HAS_USER", 1.0, gid_pub);
                    add_edge(gid_pub, "MEMBER_OF", 1.0, gid_pub);
                    add_edge(uid, "MEMBER_OF", 1.0, gid_pub);

                    snowflake_id_t gid_adm = resolve_id_from_index(EntityType::User, "nz", "rodsadmin#" + std::string(zone_name));
                    if (!gid_adm) {
                        gid_adm = resolve_id_from_index(EntityType::User, "n", "rodsadmin");
                    }
                    if (!gid_adm) {
                        gid_adm = make_id(EntityType::User, 3);
                    }
                    std::string adm_payload = client_->get_node_payload_async(local_cluster_id_, gid_adm).get();
                    if (adm_payload.empty()) {
                        lite3cpp::Buffer gbuf_admin; gbuf_admin.init_object(); 
                        gbuf_admin.set_str(0, "n", "rodsadmin"); 
                        gbuf_admin.set_str(0, "t", "rodsgroup"); 
                        gbuf_admin.set_str(0, "z", std::string(zone_name));
                        gbuf_admin.set_str(0, "c", "");
                        gbuf_admin.set_str(0, "i", "");
                        gbuf_admin.set_str(0, "ct", time_buf);
                        gbuf_admin.set_str(0, "mt", time_buf);
                        gbuf_admin.set_i64(0, "id", 3);
                        client_->put_node_async(local_cluster_id_, gid_adm, gbuf_admin.move_to_string()).get();
                    }
                    add_index(EntityType::User, "nz", "rodsadmin#" + std::string(zone_name), gid_adm);
                    add_index(EntityType::User, "n", "rodsadmin", gid_adm);
                    add_index(EntityType::User, "id", "3", gid_adm);
                    add_edge(zid, "HAS_USER", 1.0, gid_adm);
                    add_edge(gid_adm, "MEMBER_OF", 1.0, gid_adm);
                    add_edge(uid, "MEMBER_OF", 1.0, gid_adm);

                    std::string def_resc = cfg.default_resc.empty() ? "demoResc" : cfg.default_resc;
                    std::string def_vault = cfg.default_resc_vault.empty() ? "/var/lib/irods/Vault" : cfg.default_resc_vault;
                    snowflake_id_t resc_sid = resolve_id_from_index(EntityType::Resource, "n", def_resc);
                    if (!resc_sid) {
                        resc_sid = make_id(EntityType::Resource, 40001);
                    }
                    std::string rpayload = client_->get_node_payload_async(local_cluster_id_, resc_sid).get();
                    if (rpayload.empty()) {
                        char time_buf[32];
                        snprintf(time_buf, sizeof(time_buf), "%011lld", (long long)time(nullptr));
                        char hostname[1024] = {0};
                        gethostname(hostname, sizeof(hostname));
                        lite3cpp::Buffer rbuf; rbuf.init_object();
                        rbuf.set_i64(0, "id", 40001);
                        rbuf.set_str(0, "n", def_resc);
                        rbuf.set_str(0, "z", std::string(zone_name));
                        rbuf.set_str(0, "t", "unixfilesystem");
                        rbuf.set_str(0, "l", hostname);
                        rbuf.set_str(0, "v", def_vault);
                        rbuf.set_str(0, "cx", "");
                        rbuf.set_str(0, "m", "");
                        rbuf.set_i64(0, "f", 0);
                        rbuf.set_i64(0, "s", 1);
                        rbuf.set_str(0, "ct", time_buf);
                        rbuf.set_str(0, "mt", time_buf);
                        rbuf.set_str(0, "entity_type", "resource");
                        rbuf.set_str(0, "ch", "");
                        rbuf.set_str(0, "p", "");
                        rbuf.set_str(0, "pc", "");
                        client_->put_node_async(local_cluster_id_, resc_sid, rbuf.move_to_string()).get();
                    }
                    add_index(EntityType::Resource, "n", def_resc, resc_sid);
                    add_index(EntityType::Resource, "id", "40001", resc_sid);
                    add_edge(zid, "HAS_RESC", 1.0, resc_sid);

                    char coll_time_buf[32];
                    snprintf(coll_time_buf, sizeof(coll_time_buf), "%011lld", (long long)time(nullptr));
                    auto ensure_coll = [&](std::string_view coll_name, std::string_view parent_name, uint64_t coll_id, snowflake_id_t parent_sid, bool set_own) -> snowflake_id_t {
                        snowflake_id_t cid = resolve_id_from_index(EntityType::Collection, "n", coll_name);
                        if (!cid) {
                            cid = make_id(EntityType::Collection, coll_id);
                        }
                        std::string cpayload = client_->get_node_payload_async(local_cluster_id_, cid).get();
                        bool needs_update = false;
                        lite3cpp::Buffer cbuf;
                        if (cpayload.empty()) {
                            cbuf.init_object();
                            cbuf.set_str(0, "entity_type", "collection");
                            cbuf.set_str(0, "t", "");
                            cbuf.set_str(0, "n", std::string(coll_name));
                            cbuf.set_str(0, "pn", std::string(parent_name));
                            cbuf.set_str(0, "o", adm_user);
                            cbuf.set_str(0, "z", std::string(zone_name));
                            cbuf.set_i64(0, "id", static_cast<int64_t>(coll_id));
                            cbuf.set_str(0, "ct", coll_time_buf);
                            cbuf.set_str(0, "mt", coll_time_buf);
                            needs_update = true;
                        } else {
                            try {
                                lite3cpp::Buffer exist_buf(cpayload);
                                std::string et = safe_get_str(exist_buf, 0, "entity_type");
                                std::string t = safe_get_str(exist_buf, 0, "t");
                                std::string ct = safe_get_str(exist_buf, 0, "ct");
                                bool dirty = false;
                                if (et != "collection") {
                                    exist_buf.set_str(0, "entity_type", "collection");
                                    dirty = true;
                                }
                                if (t == "collection") {
                                    exist_buf.set_str(0, "t", "");
                                    dirty = true;
                                }
                                if (ct.empty()) {
                                    exist_buf.set_str(0, "ct", coll_time_buf);
                                    exist_buf.set_str(0, "mt", coll_time_buf);
                                    dirty = true;
                                }
                                if (dirty) {
                                    cbuf = std::move(exist_buf);
                                    needs_update = true;
                                }
                            } catch (...) {}
                        }
                        if (needs_update) {
                            client_->put_node_async(local_cluster_id_, cid, cbuf.move_to_string()).get();
                        }
                        add_index(EntityType::Collection, "n", coll_name, cid);
                        add_index(EntityType::Collection, "id", std::to_string(coll_id), cid);
                        if (parent_sid) {
                            add_edge(parent_sid, "CONTAINS", 1.0, cid);
                        }
                        if (set_own) {
                            set_access(adm_user, zone_name, coll_name, "own", false);
                        }
                        return cid;
                    };

                    snowflake_id_t sys_root_cid = ensure_coll("/", "/", 9999, 0, true);
                    add_edge(zid, "HAS_ROOT_COLL", 1.0, sys_root_cid);

                    std::string root_coll_name = "/" + std::string(zone_name);
                    snowflake_id_t rcid = ensure_coll(root_coll_name, "/", 1, sys_root_cid, true);
                    add_edge(zid, "HAS_ROOT_COLL", 1.0, rcid);

                    std::string home_coll_name = root_coll_name + "/home";
                    snowflake_id_t hcid = ensure_coll(home_coll_name, root_coll_name, 2, rcid, true);

                    std::string public_coll_name = home_coll_name + "/public";
                    ensure_coll(public_coll_name, home_coll_name, 3, hcid, false);
                    set_access("public", zone_name, public_coll_name, "own", false);
                    set_access(adm_user, zone_name, public_coll_name, "own", false);

                    std::string adm_coll_name = home_coll_name + "/" + adm_user;
                    ensure_coll(adm_coll_name, home_coll_name, 6, hcid, true);

                    std::string trash_coll_name = root_coll_name + "/trash";
                    snowflake_id_t tcid = ensure_coll(trash_coll_name, root_coll_name, 4, rcid, true);

                    std::string trash_home_coll_name = trash_coll_name + "/home";
                    snowflake_id_t thcid = ensure_coll(trash_home_coll_name, trash_coll_name, 5, tcid, true);

                    std::string trash_public_coll_name = trash_home_coll_name + "/public";
                    ensure_coll(trash_public_coll_name, trash_home_coll_name, 7, thcid, false);
                    set_access("public", zone_name, trash_public_coll_name, "own", false);
                    set_access(adm_user, zone_name, trash_public_coll_name, "own", false);

                }

                snowflake_id_t marker_id = SnowflakeID::create(local_cluster_id_, "init:default_specific_queries");
                std::string marker_payload = client_->get_node_payload_async(local_cluster_id_, marker_id).get();
                if (marker_payload.empty()) {
                    register_specific_query("ShowCollAcls", "select distinct R_USER_MAIN.user_name, R_USER_MAIN.zone_name, R_TOKN_MAIN.token_name, R_USER_MAIN.user_type_name from R_USER_MAIN, R_TOKN_MAIN, R_OBJT_ACCESS, R_COLL_MAIN where R_OBJT_ACCESS.object_id = R_COLL_MAIN.coll_id AND R_COLL_MAIN.coll_name = ? AND R_TOKN_MAIN.token_namespace = 'access_type' AND R_USER_MAIN.user_id = R_OBJT_ACCESS.user_id AND R_OBJT_ACCESS.access_type_id = R_TOKN_MAIN.token_id");
                    register_specific_query("ls", "select alias, sqlStr from R_SPECIFIC_QUERY");
                    register_specific_query("lsl", "select alias, sqlStr from R_SPECIFIC_QUERY where sqlStr like ?");
                    client_->put_node_async(local_cluster_id_, marker_id, "{\"initialized\":true}").get();
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
             client_->put_edge_async(local_cluster_id_, idx_key, id_hex).get();
             if (type == EntityType::DataObject && attr == "n") {
                 std::string multi_idx_key = idx_key + ":" + std::string(id_hex);
                 client_->put_edge_async(local_cluster_id_, multi_idx_key, id_hex).get();
             }
        }


        void del_index(EntityType type, std::string_view attr, std::string_view value, snowflake_id_t target_id = 0) {
             std::string idx_key = get_idx_key(type, attr, value);
             client_->del_edge_async(local_cluster_id_, idx_key).get();
             if (type == EntityType::DataObject && attr == "n" && target_id != 0) {
                 char id_hex[17];
                 std::snprintf(id_hex, sizeof(id_hex), "%016llx", (unsigned long long)target_id);
                 client_->del_edge_async(local_cluster_id_, idx_key + ":" + std::string(id_hex)).get();
             }
        }

        snowflake_id_t resolve_id_from_index(EntityType type, std::string_view attr, std::string_view value) {
             try {
                 std::string idx_key = get_idx_key(type, attr, value);
                 auto fut = client_->get_raw_key_async(local_cluster_id_, idx_key);
                 std::string payload = fut.get();
                 if (payload.empty()) return 0;
                 return std::stoull(payload, nullptr, 16);
             } catch (...) {
                 return 0;
             }
        }

        snowflake_id_t get_zone_id(std::string_view zname = "") const {
            std::string_view target_zone = zname.empty() ? std::string_view(local_zone_name_) : zname;
            if (!target_zone.empty()) {
                snowflake_id_t zid = const_cast<CatalogImpl*>(this)->resolve_id_from_index(EntityType::Zone, "n", target_zone);
                if (zid) return zid;
            }
            return const_cast<CatalogImpl*>(this)->make_id(EntityType::Zone, 1);
        }

        const std::string& get_local_zone_name() const {
            return local_zone_name_;
        }

        snowflake_id_t resolve_user(std::string_view user_name, std::string_view zone = "") {
            std::string clean_name(user_name);
            std::string clean_zone(zone);
            auto hpos = clean_name.find('#');
            if (hpos != std::string::npos) {
                clean_zone = clean_name.substr(hpos + 1);
                clean_name = clean_name.substr(0, hpos);
            }
            if (clean_zone.empty()) {
                clean_zone = local_zone_name_;
            }

            snowflake_id_t uid = resolve_id_from_index(EntityType::User, "nz", clean_name + "#" + clean_zone);
            if (uid) return uid;

            if (clean_zone == local_zone_name_ || clean_zone.empty()) {
                uid = resolve_id_from_index(EntityType::User, "n", clean_name);
                if (uid) return uid;
            }

            if (clean_zone.empty()) {
                uid = resolve_id_from_index(EntityType::User, "n", user_name);
            }
            return uid;
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
            add_edge(zid, "HAS_ZONE", 1.0, zid);
            
            snowflake_id_t uid = make_id(EntityType::User, 1);
            lite3cpp::Buffer ubuf; ubuf.init_object(); 
            ubuf.set_str(0, "n", std::string(admin_name)); 
            ubuf.set_str(0, "t", "rodsadmin"); 
            ubuf.set_str(0, "z", std::string(zone_name));
            ubuf.set_i64(0, "p", 5); 
            ubuf.set_str(0, "pw", std::string(admin_name));
            ubuf.set_str(0, "c", "");
            ubuf.set_str(0, "i", "");
            ubuf.set_str(0, "ct", time_buf);
            ubuf.set_str(0, "mt", time_buf);
            ubuf.set_i64(0, "id", 1);
            client_->put_node_async(local_cluster_id_, uid, ubuf.move_to_string()).get();
            add_index(EntityType::User, "nz", std::string(admin_name) + "#" + std::string(zone_name), uid);
            add_index(EntityType::User, "n", admin_name, uid);
            add_index(EntityType::User, "id", "1", uid);
            add_edge(zid, "HAS_USER", 1.0, uid);
            add_edge(uid, "MEMBER_OF", 1.0, uid);

            // Bootstrap default groups
            snowflake_id_t gid_public = make_id(EntityType::User, 2);
            lite3cpp::Buffer gbuf_public; gbuf_public.init_object(); 
            gbuf_public.set_str(0, "n", "public"); 
            gbuf_public.set_str(0, "t", "rodsgroup"); 
            gbuf_public.set_str(0, "z", std::string(zone_name));
            gbuf_public.set_str(0, "c", "");
            gbuf_public.set_str(0, "i", "");
            gbuf_public.set_str(0, "ct", time_buf);
            gbuf_public.set_str(0, "mt", time_buf);
            gbuf_public.set_i64(0, "id", 2);
            client_->put_node_async(local_cluster_id_, gid_public, gbuf_public.move_to_string()).get();
            add_index(EntityType::User, "nz", "public#" + std::string(zone_name), gid_public);
            add_index(EntityType::User, "n", "public", gid_public);
            add_index(EntityType::User, "id", "2", gid_public);
            add_edge(zid, "HAS_USER", 1.0, gid_public);
            add_edge(gid_public, "MEMBER_OF", 1.0, gid_public);

            snowflake_id_t gid_admin = make_id(EntityType::User, 3);
            lite3cpp::Buffer gbuf_admin; gbuf_admin.init_object(); 
            gbuf_admin.set_str(0, "n", "rodsadmin"); 
            gbuf_admin.set_str(0, "t", "rodsgroup"); 
            gbuf_admin.set_str(0, "z", std::string(zone_name));
            gbuf_admin.set_str(0, "c", "");
            gbuf_admin.set_str(0, "i", "");
            gbuf_admin.set_str(0, "ct", time_buf);
            gbuf_admin.set_str(0, "mt", time_buf);
            gbuf_admin.set_i64(0, "id", 3);
            client_->put_node_async(local_cluster_id_, gid_admin, gbuf_admin.move_to_string()).get();
            add_index(EntityType::User, "nz", "rodsadmin#" + std::string(zone_name), gid_admin);
            add_index(EntityType::User, "n", "rodsadmin", gid_admin);
            add_index(EntityType::User, "id", "3", gid_admin);
            add_edge(zid, "HAS_USER", 1.0, gid_admin);
            add_edge(gid_admin, "MEMBER_OF", 1.0, gid_admin);

            // Add rods user to rodsadmin and public groups
            add_edge(uid, "MEMBER_OF", 1.0, gid_admin);
            add_edge(uid, "MEMBER_OF", 1.0, gid_public);

            char hostname[1024];
            if (gethostname(hostname, sizeof(hostname)) == 0) {
                set_grid_configuration_value("delay_server:leader", hostname);
                set_grid_configuration_value("delay_server:successor", "");
            }

            // Register Standard Collections
            // True Root Collection "/"
            std::string sys_root_coll = "/";
            snowflake_id_t sys_root_cid = make_id(EntityType::Collection, 9999);
            lite3cpp::Buffer cbuf;
            cbuf.init_object();
            cbuf.set_str(0, "entity_type", "collection");
            cbuf.set_str(0, "t", "");
            cbuf.set_str(0, "n", sys_root_coll); cbuf.set_str(0, "pn", "/");
            cbuf.set_str(0, "o", std::string(admin_name)); cbuf.set_str(0, "z", std::string(zone_name));
            cbuf.set_i64(0, "id", 9999);
            cbuf.set_str(0, "ct", time_buf); cbuf.set_str(0, "mt", time_buf);
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
            cbuf.init_object();
            cbuf.set_str(0, "entity_type", "collection");
            cbuf.set_str(0, "t", "");
            cbuf.set_str(0, "n", root_coll_name); cbuf.set_str(0, "pn", "/");
            cbuf.set_str(0, "o", std::string(admin_name)); cbuf.set_str(0, "z", std::string(zone_name));
            cbuf.set_i64(0, "id", 1);
            cbuf.set_str(0, "ct", time_buf); cbuf.set_str(0, "mt", time_buf);
            client_->put_node_async(local_cluster_id_, rcid, cbuf.move_to_string()).get();
            add_index(EntityType::Collection, "n", root_coll_name, rcid);
            add_index(EntityType::Collection, "id", "1", rcid);
            add_edge(sys_root_cid, "CONTAINS", 1.0, rcid);
            add_edge(zid, "HAS_ROOT_COLL", 1.0, rcid);
            set_access(admin_name, zone_name, root_coll_name, "own", false);
            
            // home
            cbuf.init_object();
            cbuf.set_str(0, "entity_type", "collection");
            cbuf.set_str(0, "t", "");
            cbuf.set_str(0, "n", home_coll_name); cbuf.set_str(0, "pn", root_coll_name);
            cbuf.set_str(0, "o", std::string(admin_name)); cbuf.set_str(0, "z", std::string(zone_name));
            cbuf.set_i64(0, "id", 2);
            cbuf.set_str(0, "ct", time_buf); cbuf.set_str(0, "mt", time_buf);
            client_->put_node_async(local_cluster_id_, hcid, cbuf.move_to_string()).get();
            add_index(EntityType::Collection, "n", home_coll_name, hcid);
            add_index(EntityType::Collection, "id", "2", hcid);
            add_edge(rcid, "CONTAINS", 1.0, hcid);
            set_access(admin_name, zone_name, home_coll_name, "own", false);
            
            // public
            cbuf.init_object();
            cbuf.set_str(0, "entity_type", "collection");
            cbuf.set_str(0, "t", "");
            cbuf.set_str(0, "n", public_coll_name); cbuf.set_str(0, "pn", home_coll_name);
            cbuf.set_str(0, "o", std::string(admin_name)); cbuf.set_str(0, "z", std::string(zone_name));
            cbuf.set_i64(0, "id", 3);
            cbuf.set_str(0, "ct", time_buf); cbuf.set_str(0, "mt", time_buf);
            client_->put_node_async(local_cluster_id_, pcid, cbuf.move_to_string()).get();
            add_index(EntityType::Collection, "n", public_coll_name, pcid);
            add_index(EntityType::Collection, "id", "3", pcid);
            add_edge(hcid, "CONTAINS", 1.0, pcid);
            set_access("public", zone_name, public_coll_name, "own", false);
            set_access(admin_name, zone_name, public_coll_name, "own", false);

            // home/<admin_name> (admin home)
            std::string rods_coll_name = home_coll_name + "/" + std::string(admin_name);
            snowflake_id_t rodscid = make_id(EntityType::Collection, 6);
            cbuf.init_object();
            cbuf.set_str(0, "entity_type", "collection");
            cbuf.set_str(0, "t", "");
            cbuf.set_str(0, "n", rods_coll_name); cbuf.set_str(0, "pn", home_coll_name);
            cbuf.set_str(0, "o", std::string(admin_name)); cbuf.set_str(0, "z", std::string(zone_name));
            cbuf.set_i64(0, "id", 6);
            cbuf.set_str(0, "ct", time_buf); cbuf.set_str(0, "mt", time_buf);
            client_->put_node_async(local_cluster_id_, rodscid, cbuf.move_to_string()).get();
            add_index(EntityType::Collection, "n", rods_coll_name, rodscid);
            add_index(EntityType::Collection, "id", "6", rodscid);
            add_edge(hcid, "CONTAINS", 1.0, rodscid);
            set_access(admin_name, zone_name, rods_coll_name, "own", false);

            // trash
            std::string trash_coll_name = root_coll_name + "/trash";
            snowflake_id_t tcid = make_id(EntityType::Collection, 4);
            cbuf.init_object();
            cbuf.set_str(0, "entity_type", "collection");
            cbuf.set_str(0, "t", "");
            cbuf.set_str(0, "n", trash_coll_name); cbuf.set_str(0, "pn", root_coll_name);
            cbuf.set_str(0, "o", std::string(admin_name)); cbuf.set_str(0, "z", std::string(zone_name));
            cbuf.set_i64(0, "id", 4);
            cbuf.set_str(0, "ct", time_buf); cbuf.set_str(0, "mt", time_buf);
            client_->put_node_async(local_cluster_id_, tcid, cbuf.move_to_string()).get();
            add_index(EntityType::Collection, "n", trash_coll_name, tcid);
            add_index(EntityType::Collection, "id", "4", tcid);
            add_edge(rcid, "CONTAINS", 1.0, tcid);
            set_access(admin_name, zone_name, trash_coll_name, "own", false);

            // trash/home
            std::string trash_home_coll_name = trash_coll_name + "/home";
            snowflake_id_t thcid = make_id(EntityType::Collection, 5);
            cbuf.init_object();
            cbuf.set_str(0, "entity_type", "collection");
            cbuf.set_str(0, "t", "");
            cbuf.set_str(0, "n", trash_home_coll_name); cbuf.set_str(0, "pn", trash_coll_name);
            cbuf.set_str(0, "o", std::string(admin_name)); cbuf.set_str(0, "z", std::string(zone_name));
            cbuf.set_i64(0, "id", 5);
            cbuf.set_str(0, "ct", time_buf); cbuf.set_str(0, "mt", time_buf);
            client_->put_node_async(local_cluster_id_, thcid, cbuf.move_to_string()).get();
            add_index(EntityType::Collection, "n", trash_home_coll_name, thcid);
            add_index(EntityType::Collection, "id", "5", thcid);
            add_edge(tcid, "CONTAINS", 1.0, thcid);
            set_access(admin_name, zone_name, trash_home_coll_name, "own", false);

            // trash/home/public
            std::string trash_public_coll_name = trash_home_coll_name + "/public";
            snowflake_id_t tpcid = make_id(EntityType::Collection, 7);
            cbuf.init_object();
            cbuf.set_str(0, "entity_type", "collection");
            cbuf.set_str(0, "t", "");
            cbuf.set_str(0, "n", trash_public_coll_name); cbuf.set_str(0, "pn", trash_home_coll_name);
            cbuf.set_str(0, "o", std::string(admin_name)); cbuf.set_str(0, "z", std::string(zone_name));
            cbuf.set_i64(0, "id", 7);
            cbuf.set_str(0, "ct", time_buf); cbuf.set_str(0, "mt", time_buf);
            client_->put_node_async(local_cluster_id_, tpcid, cbuf.move_to_string()).get();
            add_index(EntityType::Collection, "n", trash_public_coll_name, tpcid);
            add_index(EntityType::Collection, "id", "7", tpcid);
            add_edge(thcid, "CONTAINS", 1.0, tpcid);
            set_access("public", zone_name, trash_public_coll_name, "own", false);
            set_access(admin_name, zone_name, trash_public_coll_name, "own", false);

            // Register default specific queries
            register_specific_query("ls", "select alias, sqlStr from R_SPECIFIC_QUERY");
            register_specific_query("lsl", "select alias, sqlStr from R_SPECIFIC_QUERY where sqlStr like ?");
            register_specific_query("ShowCollAcls", "select distinct R_USER_MAIN.user_name, R_USER_MAIN.zone_name, R_TOKN_MAIN.token_name, R_USER_MAIN.user_type_name from R_USER_MAIN, R_TOKN_MAIN, R_OBJT_ACCESS, R_COLL_MAIN where R_OBJT_ACCESS.object_id = R_COLL_MAIN.coll_id AND R_COLL_MAIN.coll_name = ? AND R_TOKN_MAIN.token_namespace = 'access_type' AND R_USER_MAIN.user_id = R_OBJT_ACCESS.user_id AND R_OBJT_ACCESS.access_type_id = R_TOKN_MAIN.token_id");

            return SUCCESS();
        }

        // --- Data Object Operations ---
        irods::error register_data_object(const data_object& obj, data_id_t& out_id, const replica* initial_repl = nullptr) {
            std::string full_path = obj.full_path;
            if (full_path.empty() && obj.coll_id != 0) {
                snowflake_id_t csid = make_id(EntityType::Collection, obj.coll_id);
                std::string cname;
                {
                    std::lock_guard<std::mutex> lock(s_reg_cache_mu);
                    auto it = s_coll_path_cache.find(csid);
                    if (it != s_coll_path_cache.end()) {
                        cname = it->second;
                    }
                }
                if (cname.empty()) {
                    std::string cpayload = client_->get_node_payload_async(local_cluster_id_, csid).get();
                    if (!cpayload.empty()) {
                        try {
                            lite3cpp::Buffer cbuf(cpayload);
                            cname = safe_get_str(cbuf, 0, "n");
                            if (!cname.empty()) {
                                std::lock_guard<std::mutex> lock(s_reg_cache_mu);
                                s_coll_path_cache[csid] = cname;
                            }
                        } catch (...) {}
                    }
                }
                if (!cname.empty()) {
                    if (cname.back() == '/') {
                        full_path = cname + obj.name;
                    } else {
                        full_path = cname + "/" + obj.name;
                    }
                }
            }

            // Check if collection exists with this path
            if (!full_path.empty()) {
                bool coll_exists = false;
                {
                    std::lock_guard<std::mutex> lock(s_path_cache_mu);
                    if (s_coll_name_cache.find(full_path) != s_coll_name_cache.end()) {
                        coll_exists = true;
                    }
                }
                if (coll_exists) {
                    return ERROR(CAT_NAME_EXISTS_AS_COLLECTION, "Collection already exists with data object name: " + full_path);
                }
                snowflake_id_t existing_coll = resolve_id_from_index(EntityType::Collection, "n", full_path);
                if (existing_coll) {
                    return ERROR(CAT_NAME_EXISTS_AS_COLLECTION, "Collection already exists with data object name: " + full_path);
                }
            }

            snowflake_id_t sid = make_id(EntityType::DataObject, obj.id);
            #ifdef IRODS_SERVER
            rodsLog(LOG_DEBUG, "L3_CATALOG: Registering DataObject [%s] with ID [%llu] (SID: %016llx) in Coll [%llu]", obj.name.c_str(), (unsigned long long)obj.id, (unsigned long long)sid, (unsigned long long)obj.coll_id);
            #endif
            std::string user_key = obj.owner_name + "#" + (obj.owner_zone.empty() ? local_zone_name_ : obj.owner_zone);
            snowflake_id_t uid = 0;
            {
                std::lock_guard<std::mutex> lock(s_reg_cache_mu);
                auto it = s_user_id_cache.find(user_key);
                if (it != s_user_id_cache.end()) {
                    uid = it->second;
                }
            }
            if (!uid) {
                uid = resolve_user(obj.owner_name, obj.owner_zone);
                if (uid) {
                    std::lock_guard<std::mutex> lock(s_reg_cache_mu);
                    s_user_id_cache[user_key] = uid;
                }
            }

            lite3cpp::Buffer buf; buf.init_object(); 
            buf.set_str(0, "n", obj.name); buf.set_str(0, "o", obj.owner_name); buf.set_i64(0, "s", obj.size); 
            buf.set_str(0, "t", obj.type);
            buf.set_str(0, "entity_type", "data_object");
            buf.set_str(0, "p", full_path);
            std::string parent_coll = full_path.substr(0, full_path.rfind('/'));
            buf.set_str(0, "pn", (parent_coll.empty() ? "/" : parent_coll));
            buf.set_str(0, "ct", obj.create_ts); buf.set_str(0, "mt", obj.modify_ts);
            buf.set_i64(0, "id", static_cast<int64_t>(obj.id));
            buf.set_i64(0, "cid", static_cast<int64_t>(obj.coll_id));
            if (uid) {
                buf.set_i64(0, "uid", static_cast<int64_t>(uid));
            }
            std::string expiry = obj.expiry.empty() ? "00000000000" : obj.expiry;
            buf.set_str(0, "ex", expiry);
            if (!obj.owner_zone.empty()) buf.set_str(0, "z", obj.owner_zone);
            if (!obj.mode.empty()) buf.set_str(0, "mode", obj.mode);
            if (!obj.version.empty()) buf.set_str(0, "v", obj.version);
            if (!obj.comments.empty()) buf.set_str(0, "c", obj.comments);
            if (!obj.status.empty()) buf.set_str(0, "st", obj.status);
            l3kvg::MutationBatch batch;
            batch.put_node(sid, buf.move_to_string());

            char id_hex[17];
            std::snprintf(id_hex, sizeof(id_hex), "%016llx", (unsigned long long)sid);

            std::string idx_name = get_idx_key(EntityType::DataObject, "n", obj.name);
            batch.put_raw(idx_name, id_hex);
            batch.put_raw(idx_name + ":" + std::string(id_hex), id_hex);

            std::string idx_id = get_idx_key(EntityType::DataObject, "id", std::to_string(obj.id));
            batch.put_raw(idx_id, id_hex);

            if (!full_path.empty()) {
                std::string idx_path = get_idx_key(EntityType::DataObject, "path", full_path);
                batch.put_raw(idx_path, id_hex);
            }

            std::string idx_pn = get_idx_key(EntityType::DataObject, "pn", (parent_coll.empty() ? "/" : parent_coll));
            batch.put_raw(idx_pn + ":" + std::string(id_hex), id_hex);
            
            snowflake_id_t cid = make_id(EntityType::Collection, obj.coll_id);
            #ifdef IRODS_SERVER
            rodsLog(LOG_DEBUG, "L3_CATALOG: Creating CONTAINS edge: %016llx -- CONTAINS --> %016llx", (unsigned long long)cid, (unsigned long long)sid);
            #endif
            batch.add_edge(cid, "CONTAINS", 1.0, sid, "{}");

            if (uid) {
                #ifdef IRODS_SERVER
                rodsLog(LOG_DEBUG, "L3_CATALOG: Creating OWNS edge: %016llx -- OWNS --> %016llx", (unsigned long long)uid, (unsigned long long)sid);
                #endif
                batch.add_edge(uid, "OWNS", 1.0, sid, "{}");

                // Owner access
                std::string aid_uuid = std::to_string(uid) + ":" + std::to_string(sid);
                snowflake_id_t aid = SnowflakeID::create(local_cluster_id_, aid_uuid);

                lite3cpp::Buffer abuf; abuf.init_object();
                abuf.set_str(0, "l", "own");
                abuf.set_str(0, "t", "access_type");
                abuf.set_str(0, "entity_type", "access");
                abuf.set_str(0, "u", obj.owner_name);
                std::string zone_str = obj.owner_zone.empty() ? local_zone_name_ : obj.owner_zone;
                abuf.set_str(0, "z", zone_str);
                abuf.set_i64(0, "uid", static_cast<int64_t>(uid));

                batch.put_node(aid, abuf.move_to_string());
                batch.add_edge(uid, "HAS_ACCESS", 1.0, aid, "{}");

                std::vector<snowflake_id_t> members;
                bool cache_hit = false;
                auto now = std::chrono::steady_clock::now();
                {
                    std::lock_guard<std::mutex> lock(s_user_members_mu);
                    auto it = s_user_members_cache.find(uid);
                    if (it != s_user_members_cache.end() && now < it->second.expires_at) {
                        members = it->second.members;
                        cache_hit = true;
                    }
                }
                if (!cache_hit) {
                    members = client_->get_in_neighbors_async(local_cluster_id_, uid, "MEMBER_OF").get();
                    std::lock_guard<std::mutex> lock(s_user_members_mu);
                    s_user_members_cache[uid] = {members, now + std::chrono::seconds(30)};
                }
                for (auto mid : members) {
                    batch.add_edge(mid, "HAS_ACCESS", 1.0, aid, "{}");
                }
                batch.add_edge(aid, "FOR_OBJECT", 1.0, sid, "{}");
            }

            if (initial_repl) {
                uint64_t r_data_id = initial_repl->data_id != 0 ? initial_repl->data_id : obj.id;
                std::string local_uuid = std::to_string(r_data_id) + ":" + std::to_string(initial_repl->replica_number);
                snowflake_id_t rid = SnowflakeID::create(local_cluster_id_, local_uuid);
                lite3cpp::Buffer r_buf; r_buf.init_object();
                r_buf.set_i64(0, "id", static_cast<int64_t>(r_data_id));
                r_buf.set_str(0, "p", initial_repl->physical_path);
                r_buf.set_str(0, "rh", initial_repl->resc_hier);
                r_buf.set_str(0, "st", initial_repl->status);
                r_buf.set_str(0, "c", initial_repl->checksum);
                r_buf.set_str(0, "cs", initial_repl->checksum);
                r_buf.set_str(0, "mt", initial_repl->modify_ts);
                r_buf.set_str(0, "t", "replica");
                r_buf.set_str(0, "entity_type", "replica");
                r_buf.set_i64(0, "s", initial_repl->size);
                r_buf.set_i64(0, "rn", static_cast<int64_t>(initial_repl->replica_number));
                r_buf.set_i64(0, "rid", static_cast<int64_t>(initial_repl->resource_id));
                batch.put_node(rid, r_buf.move_to_string());
                batch.add_edge(sid, "HAS_REPLICA", 1.0, rid, "{}");
                if (initial_repl->resource_id != 0) {
                    snowflake_id_t resc_sid = make_id(EntityType::Resource, initial_repl->resource_id);
                    batch.add_edge(resc_sid, "HOSTS_REPLICA", 1.0, rid, "{}");
                    batch.add_edge(rid, "STAYING_AT", 1.0, resc_sid, "{}");
                }
            }

            client_->execute_batch_async(local_cluster_id_, batch).get();

            out_id = obj.id; return SUCCESS();
        }
        irods::error delete_data_object(data_id_t id) { 
            try {
                snowflake_id_t sid = make_id(EntityType::DataObject, id);
                
                #ifdef IRODS_SERVER
                rodsLog(LOG_DEBUG, "L3_CATALOG: Deleting DataObject %llu (SID: %016llx)", (unsigned long long)id, (unsigned long long)sid);
                #endif

                l3kvg::MutationBatch batch;

                // Roundtrip 1: Pipelined parallel query futures
                auto f_payload = client_->get_node_payload_async(local_cluster_id_, sid);
                auto f_repl = client_->get_neighbors_async(local_cluster_id_, sid, "HAS_REPLICA", 0.0);
                auto f_access = client_->get_in_neighbors_async(local_cluster_id_, sid, "FOR_OBJECT");
                auto f_avus = client_->get_neighbors_async(local_cluster_id_, sid, "ANNOTATED_WITH", 0.0);

                std::string payload = f_payload.get();
                int64_t cid = 0;
                int64_t uid = 0;
                std::string owner_name, owner_zone;
                std::string pn;

                if (!payload.empty()) {
                    try {
                        lite3cpp::Buffer buf(reinterpret_cast<const uint8_t*>(payload.data()), payload.size());
                        std::string name = safe_get_str(buf, 0, "n");
                        char id_hex[17];
                        std::snprintf(id_hex, sizeof(id_hex), "%016llx", (unsigned long long)sid);

                        if (!name.empty()) {
                            std::string idx_name = get_idx_key(EntityType::DataObject, "n", name);
                            batch.del_raw(idx_name);
                            batch.del_raw(idx_name + ":" + std::string(id_hex));
                        }
                        std::string id_str = safe_get_str(buf, 0, "id");
                        if (!id_str.empty()) {
                            batch.del_raw(get_idx_key(EntityType::DataObject, "id", id_str));
                        } else if (id != 0) {
                            batch.del_raw(get_idx_key(EntityType::DataObject, "id", std::to_string(id)));
                        }

                        std::string path = safe_get_str(buf, 0, "p");
                        if (!path.empty()) {
                            batch.del_raw(get_idx_key(EntityType::DataObject, "path", path));
                        }

                        pn = safe_get_str(buf, 0, "pn");
                        if (pn.empty() && !path.empty()) {
                            size_t slash = path.rfind('/');
                            pn = (slash == 0) ? "/" : (slash != std::string::npos ? path.substr(0, slash) : "");
                        }
                        if (!pn.empty()) {
                            batch.del_raw(get_idx_key(EntityType::DataObject, "pn", pn) + ":" + std::string(id_hex));
                        }

                        try { cid = buf.get_i64(0, "cid"); } catch (...) {}
                        try { uid = buf.get_i64(0, "uid"); } catch (...) {}
                        owner_name = safe_get_str(buf, 0, "o");
                        owner_zone = safe_get_str(buf, 0, "z");
                    } catch (...) {}
                }

                // Fallback owner user resolution if uid wasn't in payload
                if (uid == 0 && !owner_name.empty()) {
                    std::string user_key = owner_name + "#" + (owner_zone.empty() ? local_zone_name_ : owner_zone);
                    {
                        std::lock_guard<std::mutex> lock(s_reg_cache_mu);
                        auto it = s_user_id_cache.find(user_key);
                        if (it != s_user_id_cache.end()) {
                            uid = it->second;
                        }
                    }
                    if (uid == 0) {
                        uid = resolve_user(owner_name, owner_zone);
                        if (uid != 0) {
                            std::lock_guard<std::mutex> lock(s_reg_cache_mu);
                            s_user_id_cache[user_key] = uid;
                        }
                    }
                }

                // Defensive fallback for collection CONTAINS edge
                snowflake_id_t coll_sid = 0;
                if (cid != 0) {
                    coll_sid = make_id(EntityType::Collection, static_cast<coll_id_t>(cid));
                }
                if (coll_sid == 0 && !pn.empty()) {
                    {
                        std::lock_guard<std::mutex> lock(s_path_cache_mu);
                        auto it = s_coll_name_cache.find(pn);
                        if (it != s_coll_name_cache.end() && it->second != 0) {
                            coll_sid = it->second;
                            cid = coll_sid;
                        }
                    }
                    if (coll_sid == 0) {
                        coll_sid = resolve_id_from_index(EntityType::Collection, "n", pn);
                        if (coll_sid != 0) {
                            cid = coll_sid;
                        }
                    }
                }

                // Delete parent collection CONTAINS edge deterministically
                if (coll_sid != 0) {
                    batch.del_edge(coll_sid, "CONTAINS", 1.0, sid);
                }

                // Delete owner OWNS edge deterministically
                if (uid != 0) {
                    batch.del_edge(uid, "OWNS", 1.0, sid);
                }

                // Delete replicas
                auto replicas = f_repl.get();
                std::vector<std::future<std::vector<snowflake_id_t>>> f_hosts;
                f_hosts.reserve(replicas.size());
                for (auto rid : replicas) {
                    f_hosts.push_back(client_->get_in_neighbors_async(local_cluster_id_, rid, "HOSTS_REPLICA"));
                }
                for (size_t i = 0; i < replicas.size(); ++i) {
                    snowflake_id_t rid = replicas[i];
                    batch.del_edge(sid, "HAS_REPLICA", 1.0, rid);
                    // Check and delete HOSTS_REPLICA and STAYING_AT edges if present
                    try {
                        auto hosts = f_hosts[i].get();
                        for (auto resc_sid : hosts) {
                            batch.del_edge(resc_sid, "HOSTS_REPLICA", 1.0, rid);
                            batch.del_edge(rid, "STAYING_AT", 1.0, resc_sid);
                        }
                        if (hosts.empty()) {
                            auto staying = client_->get_neighbors_async(local_cluster_id_, rid, "STAYING_AT", 0.0).get();
                            for (auto resc_sid : staying) {
                                batch.del_edge(resc_sid, "HOSTS_REPLICA", 1.0, rid);
                                batch.del_edge(rid, "STAYING_AT", 1.0, resc_sid);
                            }
                        }
                    } catch (...) {}
                    batch.del_node(rid);
                }

                // Delete access nodes and HAS_ACCESS edges
                auto accesses = f_access.get();
                if (!accesses.empty()) {
                    std::vector<snowflake_id_t> members;
                    if (uid != 0) {
                        bool cache_hit = false;
                        auto now = std::chrono::steady_clock::now();
                        {
                            std::lock_guard<std::mutex> lock(s_user_members_mu);
                            auto it = s_user_members_cache.find(uid);
                            if (it != s_user_members_cache.end() && now < it->second.expires_at) {
                                members = it->second.members;
                                cache_hit = true;
                            }
                        }
                        if (!cache_hit) {
                            try {
                                members = client_->get_in_neighbors_async(local_cluster_id_, uid, "MEMBER_OF").get();
                                std::lock_guard<std::mutex> lock(s_user_members_mu);
                                s_user_members_cache[uid] = {members, now + std::chrono::seconds(30)};
                            } catch (...) {}
                        }
                    }

                    snowflake_id_t owner_aid = 0;
                    if (uid != 0) {
                        std::string aid_uuid = std::to_string(uid) + ":" + std::to_string(sid);
                        owner_aid = SnowflakeID::create(local_cluster_id_, aid_uuid);
                    }

                    // Query inbound HAS_ACCESS edges for non-owner access nodes in parallel
                    std::vector<std::pair<snowflake_id_t, std::future<std::vector<snowflake_id_t>>>> non_owner_futs;
                    for (auto aid : accesses) {
                        if (aid != owner_aid || uid == 0) {
                            non_owner_futs.emplace_back(aid, client_->get_in_neighbors_async(local_cluster_id_, aid, "HAS_ACCESS"));
                        }
                    }
                    std::unordered_map<snowflake_id_t, std::vector<snowflake_id_t>> non_owner_uids;
                    for (auto& [aid, fut] : non_owner_futs) {
                        try {
                            non_owner_uids[aid] = fut.get();
                        } catch (...) {}
                    }

                    for (auto aid : accesses) {
                        if (aid == owner_aid && uid != 0) {
                            // Fast-path: owner access node, use cached uid and group members
                            batch.del_edge(uid, "HAS_ACCESS", 1.0, aid);
                            for (auto mid : members) {
                                batch.del_edge(mid, "HAS_ACCESS", 1.0, aid);
                            }
                        } else {
                            // Non-owner access node (custom ichmod ACL): query actual inbound HAS_ACCESS edges
                            auto it = non_owner_uids.find(aid);
                            if (it != non_owner_uids.end()) {
                                for (auto u : it->second) {
                                    batch.del_edge(u, "HAS_ACCESS", 1.0, aid);
                                }
                            } else {
                                try {
                                    auto uids = client_->get_in_neighbors_async(local_cluster_id_, aid, "HAS_ACCESS").get();
                                    for (auto u : uids) {
                                        batch.del_edge(u, "HAS_ACCESS", 1.0, aid);
                                    }
                                } catch (...) {}
                            }
                        }
                        batch.del_edge(aid, "FOR_OBJECT", 1.0, sid);
                        batch.del_node(aid);
                    }
                }

                // Delete AVUs (only query incoming refs if AVUs actually exist)
                auto avus = f_avus.get();
                if (!avus.empty()) {
                    for (auto aid : avus) {
                        batch.del_edge(sid, "ANNOTATED_WITH", 1.0, aid);
                        auto refs = client_->get_in_neighbors_async(local_cluster_id_, aid, "ANNOTATED_WITH").get();
                        if (refs.size() <= 1) batch.del_node(aid);
                    }
                }

                // Roundtrip 2: Delete node and execute atomic mutation batch
                batch.del_node(sid);
                client_->execute_batch_async(local_cluster_id_, batch).get();
                return SUCCESS();
            } catch (const std::exception& e) {
                return ERROR(SYS_INTERNAL_ERR, e.what());
            } catch (...) {
                return ERROR(SYS_INTERNAL_ERR, "Unknown exception in delete_data_object");
            }
        }
        irods::error rename_data_object(data_id_t obj_id, std::string_view new_name) { 
            snowflake_id_t sid = make_id(EntityType::DataObject, obj_id);
            
            // Update node 'n' property
            std::string payload = client_->get_node_payload_async(local_cluster_id_, sid).get();
            if (!payload.empty()) {
                 lite3cpp::Buffer old_buf(payload);
                 lite3cpp::Buffer new_buf; new_buf.init_object();
                 
                 std::string old_name = safe_get_str(old_buf, 0, "n");
                 std::string owner = safe_get_str(old_buf, 0, "o");
                 int64_t size = 0;
                 try { size = old_buf.get_i64(0, "s"); } catch(...) {}
                 std::string type = safe_get_str(old_buf, 0, "t");
                 std::string old_path = safe_get_str(old_buf, 0, "p");
                 std::string old_parent_coll = safe_get_str(old_buf, 0, "pn");
                 if (old_parent_coll.empty() && !old_path.empty()) {
                     size_t old_slash = old_path.find_last_of('/');
                     old_parent_coll = (old_slash == 0) ? "/" : (old_slash != std::string::npos ? old_path.substr(0, old_slash) : "");
                 }
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
                 size_t slash_pos = new_path.find_last_of('/');
                 std::string new_parent_coll = (slash_pos != std::string::npos) ? (slash_pos == 0 ? "/" : new_path.substr(0, slash_pos)) : "";
                 if (slash_pos != std::string::npos) {
                     old_buf.set_str(0, "pn", new_parent_coll);
                 }
                 old_buf.set_str(0, "mt", mt);

                 client_->put_node_async(local_cluster_id_, sid, old_buf.move_to_string()).get();

                 if (!old_name.empty()) del_index(EntityType::DataObject, "n", old_name, sid);
                 if (!old_path.empty()) del_index(EntityType::DataObject, "path", old_path);
                 add_index(EntityType::DataObject, "n", base_name, sid);
                 add_index(EntityType::DataObject, "path", new_path, sid);

                 if (old_parent_coll != new_parent_coll) {
                     char id_hex[17];
                     std::snprintf(id_hex, sizeof(id_hex), "%016llx", (unsigned long long)sid);
                     if (!old_parent_coll.empty()) {
                         client_->del_raw_async(local_cluster_id_, get_idx_key(EntityType::DataObject, "pn", old_parent_coll) + ":" + id_hex);
                     }
                     client_->put_raw_async(local_cluster_id_, get_idx_key(EntityType::DataObject, "pn", new_parent_coll) + ":" + id_hex, id_hex);
                 }
            }
            return SUCCESS(); 
        }
        irods::error move_data_object(data_id_t obj_id, coll_id_t target_coll_id) { 
            snowflake_id_t sid = make_id(EntityType::DataObject, obj_id);
            snowflake_id_t cid = make_id(EntityType::Collection, target_coll_id);
            
            std::string target_coll_path;
            {
                std::lock_guard<std::mutex> lock(s_reg_cache_mu);
                auto it = s_coll_path_cache.find(cid);
                if (it != s_coll_path_cache.end()) {
                    target_coll_path = it->second;
                }
            }
            if (target_coll_path.empty()) {
                std::string c_payload = client_->get_node_payload_async(local_cluster_id_, cid).get();
                if (c_payload.empty()) return ERROR(CAT_UNKNOWN_COLLECTION, "Target collection not found");
                lite3cpp::Buffer cbuf(c_payload);
                target_coll_path = safe_get_str(cbuf, 0, "n");
                if (!target_coll_path.empty()) {
                    std::lock_guard<std::mutex> lock(s_reg_cache_mu);
                    s_coll_path_cache[cid] = target_coll_path;
                }
            }

            auto f_payload = client_->get_node_payload_async(local_cluster_id_, sid);
            auto f_in_colls = client_->get_in_neighbors_async(local_cluster_id_, sid, "CONTAINS");

            std::string d_payload = f_payload.get();
            if (d_payload.empty()) return ERROR(CAT_UNKNOWN_FILE, "Data object not found");
            auto in_colls = f_in_colls.get();

            lite3cpp::Buffer dbuf(d_payload);

            std::string data_name = safe_get_str(dbuf, 0, "n");
            std::string old_path = safe_get_str(dbuf, 0, "p");
            std::string old_parent_path = safe_get_str(dbuf, 0, "pn");
            if (old_parent_path.empty() && !old_path.empty()) {
                size_t old_slash = old_path.find_last_of('/');
                old_parent_path = (old_slash == 0) ? "/" : (old_slash != std::string::npos ? old_path.substr(0, old_slash) : "");
            }
            std::string new_path = (target_coll_path == "/" ? "/" : (target_coll_path.ends_with('/') ? target_coll_path : target_coll_path + "/")) + data_name;

            char time_buf[50];
            snprintf(time_buf, sizeof(time_buf), "%011lld", (long long)time(nullptr));
            dbuf.set_str(0, "p", new_path);
            dbuf.set_str(0, "pn", target_coll_path);
            dbuf.set_str(0, "mt", std::string(time_buf));
            dbuf.set_i64(0, "cid", static_cast<int64_t>(target_coll_id));

            l3kvg::MutationBatch batch;
            for (auto old_cid : in_colls) {
                batch.del_edge(old_cid, "CONTAINS", 1.0, sid);
            }
            batch.add_edge(cid, "CONTAINS", 1.0, sid);
            batch.put_node(sid, dbuf.move_to_string());
            if (!old_path.empty()) {
                batch.del_raw(get_idx_key(EntityType::DataObject, "path", old_path));
            }
            char id_hex[17];
            std::snprintf(id_hex, sizeof(id_hex), "%016llx", (unsigned long long)sid);
            batch.put_raw(get_idx_key(EntityType::DataObject, "path", new_path), id_hex);

            if (!old_parent_path.empty()) {
                batch.del_raw(get_idx_key(EntityType::DataObject, "pn", old_parent_path) + ":" + id_hex);
            }
            batch.put_raw(get_idx_key(EntityType::DataObject, "pn", target_coll_path) + ":" + id_hex, id_hex);

            client_->execute_batch_async(local_cluster_id_, batch).get();

            return SUCCESS(); 
        }
        irods::error modify_data_object(data_id_t obj_id, std::string_view prop, std::string_view value) { 
            snowflake_id_t sid = make_id(EntityType::DataObject, obj_id);
            std::string payload = client_->get_node_payload_async(local_cluster_id_, sid).get();
            if (!payload.empty()) {
                 lite3cpp::Buffer buf(payload);
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
            rodsLog(LOG_DEBUG, "L3_CATALOG: Registering Replica [DataID: %llu, Num: %u] (SID: %016llx) at Resc [%llu]", (unsigned long long)repl.data_id, repl.replica_number, (unsigned long long)rid, (unsigned long long)repl.resource_id);
            #endif
            std::string rh = repl.resc_hier;
            if (rh.empty()) {
                std::string existing_payload = client_->get_node_payload_async(local_cluster_id_, rid).get();
                if (!existing_payload.empty()) {
                    lite3cpp::Buffer ebuf(existing_payload);
                    rh = safe_get_str(ebuf, 0, "rh");
                }
            }
            if (rh.empty() && repl.resource_id > 0) {
                snowflake_id_t resc_sid = make_id(EntityType::Resource, repl.resource_id);
                std::string rpayload = client_->get_node_payload_async(local_cluster_id_, resc_sid).get();
                if (!rpayload.empty()) {
                    lite3cpp::Buffer rbuf(rpayload);
                    rh = safe_get_str(rbuf, 0, "n");
                }
            }
            lite3cpp::Buffer buf; buf.init_object(); 
            buf.set_i64(0, "id", repl.data_id);
            buf.set_i64(0, "rn", repl.replica_number); 
            buf.set_str(0, "p", repl.physical_path); 
            buf.set_str(0, "rh", rh); 
            buf.set_str(0, "t", "replica");
            buf.set_str(0, "st", repl.status); 
            buf.set_str(0, "cs", repl.checksum);
            buf.set_i64(0, "rid", repl.resource_id);
            buf.set_str(0, "mt", repl.modify_ts);
            buf.set_i64(0, "s", repl.size);
            l3kvg::MutationBatch batch;
            batch.put_node(rid, buf.move_to_string());
            snowflake_id_t data_sid = make_id(EntityType::DataObject, repl.data_id);
            snowflake_id_t resc_sid = make_id(EntityType::Resource, repl.resource_id);
            batch.add_edge(data_sid, "HAS_REPLICA", 1.0, rid, "{}");
            batch.add_edge(rid, "STAYING_AT", 1.0, resc_sid, "{}");
            batch.add_edge(resc_sid, "HOSTS_REPLICA", 1.0, rid, "{}");
            client_->execute_batch_async(local_cluster_id_, batch).get();
            
            return SUCCESS();
        }
        irods::error unregister_replica(data_id_t data_id, uint32_t repl_num) { 
            std::string local_uuid = std::to_string(data_id) + ":" + std::to_string(repl_num);
            snowflake_id_t rid = SnowflakeID::create(local_cluster_id_, local_uuid);
            snowflake_id_t sid = make_id(EntityType::DataObject, data_id);

            #ifdef IRODS_SERVER
            rodsLog(LOG_DEBUG, "L3_CATALOG: Unregistering Replica [DataID: %llu, Num: %u] (SID: %016llx)", (unsigned long long)data_id, repl_num, (unsigned long long)rid);
            #endif

            auto replicas = client_->get_neighbors_async(local_cluster_id_, sid, "HAS_REPLICA", 0.0).get();
            if (replicas.empty() || (replicas.size() == 1 && (replicas[0] == rid || repl_num == 0))) {
                return delete_data_object(data_id);
            }
            // Multiple replicas remain, remove only this replica in a single batch:
            l3kvg::MutationBatch batch;
            batch.del_edge(sid, "HAS_REPLICA", 1.0, rid);
            auto staying_at = client_->get_neighbors_async(local_cluster_id_, rid, "STAYING_AT", 0.0).get();
            for (auto resc_sid : staying_at) {
                batch.del_edge(resc_sid, "HOSTS_REPLICA", 1.0, rid);
                batch.del_edge(rid, "STAYING_AT", 1.0, resc_sid);
            }
            if (staying_at.empty()) {
                std::string r_payload = client_->get_node_payload_async(local_cluster_id_, rid).get();
                if (!r_payload.empty()) {
                    try {
                        lite3cpp::Buffer r_buf(r_payload);
                        int64_t resc_id = r_buf.get_i64(0, "rid");
                        if (resc_id > 0) {
                            snowflake_id_t resc_sid = make_id(EntityType::Resource, resc_id);
                            batch.del_edge(resc_sid, "HOSTS_REPLICA", 1.0, rid);
                        }
                    } catch (...) {}
                }
            }
            batch.del_node(rid);
            client_->execute_batch_async(local_cluster_id_, batch).get();
            return SUCCESS();
        }
        irods::error update_replica_access_time(data_id_t data_id, uint32_t repl_num, std::string_view time) { 
            std::string local_uuid = std::to_string(data_id) + ":" + std::to_string(repl_num);
            snowflake_id_t rid = SnowflakeID::create(local_cluster_id_, local_uuid);
            std::string payload = client_->get_node_payload_async(local_cluster_id_, rid).get();
            if (!payload.empty()) {
                 lite3cpp::Buffer buf(payload);
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
                lite3cpp::Buffer buf(payload);
                try {
                    int64_t rn = buf.get_i64(0, "rn");
                    if (rn > max_rn) max_rn = rn;
                } catch (...) {}
            }
            return static_cast<uint32_t>(max_rn + 1);
        }
        irods::error modify_replicas_for_data_object(
            data_id_t obj_id,
            uint32_t repl_num,
            std::string_view resc_hier,
            const std::vector<std::pair<std::string, std::string>>& updates,
            bool all_repl_status,
            bool all_replicas = false) {
            snowflake_id_t sid = make_id(EntityType::DataObject, obj_id);
            auto replicas = client_->get_neighbors_async(local_cluster_id_, sid, "HAS_REPLICA", 0.0).get();

            rodsLog(LOG_NOTICE, "L3_CATALOG: modify_replicas_for_data_object obj_id=%llu, repl_num=%u, resc_hier='%.*s', updates_count=%zu, all_replicas=%d, replica_count=%zu",
                    (unsigned long long)obj_id, repl_num, (int)resc_hier.size(), resc_hier.data(), updates.size(), all_replicas ? 1 : 0, replicas.size());

            bool update_resc_hier = false;
            for (const auto& [kw, val] : updates) {
                if (kw == "rescHier" || kw == "resc_hier" || kw == "resc_id" || kw == "rescId" || kw == "DATA_RESC_HIER" || kw == "DATA_RESC_ID") {
                    update_resc_hier = true;
                    break;
                }
            }

            for (auto rid : replicas) {
                std::string payload = client_->get_node_payload_async(local_cluster_id_, rid).get();
                if (payload.empty()) continue;
                lite3cpp::Buffer buf(payload);
                uint32_t rn = 0;
                try { rn = static_cast<uint32_t>(buf.get_i64(0, "rn")); } catch (...) {}
                std::string rh{safe_get_str(buf, 0, "rh")};
                std::string curr_mt{safe_get_str(buf, 0, "mt")};
                bool modified = false;

                bool target_matched = false;
                if (all_replicas) {
                    target_matched = true;
                } else if (!update_resc_hier && !resc_hier.empty()) {
                    std::string leaf_name = std::string(resc_hier);
                    auto sep = leaf_name.rfind(';');
                    if (sep != std::string::npos) leaf_name = leaf_name.substr(sep + 1);

                    int64_t leaf_resc_id = 0;
                    snowflake_id_t resc_sid = resolve_id_from_index(EntityType::Resource, "n", leaf_name);
                    if (resc_sid) {
                        auto rpayload = client_->get_node_payload_async(local_cluster_id_, resc_sid).get();
                        if (!rpayload.empty()) {
                            try {
                                lite3cpp::Buffer rbuf(rpayload);
                                leaf_resc_id = rbuf.get_i64(0, "id");
                            } catch (...) {}
                        }
                    }

                    int64_t replica_rid = 0;
                    try { replica_rid = buf.get_i64(0, "rid"); } catch (...) {}
                    if (rh.empty() && replica_rid > 0) {
                        snowflake_id_t resc_sid = make_id(EntityType::Resource, replica_rid);
                        auto rpayload = client_->get_node_payload_async(local_cluster_id_, resc_sid).get();
                        if (!rpayload.empty()) {
                            try {
                                lite3cpp::Buffer rbuf(rpayload);
                                rh = safe_get_str(rbuf, 0, "n");
                                buf.set_str(0, "rh", rh);
                                modified = true;
                            } catch (...) {}
                        }
                    }

                    if (rh == resc_hier ||
                        (!leaf_name.empty() && (rh == leaf_name || rh.ends_with(";" + leaf_name) || std::string(resc_hier).ends_with(";" + rh))) ||
                        (leaf_resc_id > 0 && replica_rid == leaf_resc_id)) {
                        target_matched = true;
                    }
                    rodsLog(LOG_DEBUG, "L3_CATALOG: Checking replica rn=%u rh='%s' rid=%lld vs target resc_hier='%.*s' leaf='%s' leaf_id=%lld -> target_matched=%d",
                            rn, rh.c_str(), (long long)replica_rid, (int)resc_hier.size(), resc_hier.data(), leaf_name.c_str(), (long long)leaf_resc_id, target_matched ? 1 : 0);
                } else {
                    if (rn == repl_num || replicas.size() == 1) {
                        target_matched = true;
                    }
                    rodsLog(LOG_DEBUG, "L3_CATALOG: Checking replica rn=%u vs target repl_num=%u (total replicas=%zu) -> target_matched=%d",
                            rn, repl_num, replicas.size(), target_matched ? 1 : 0);
                }

                if (all_repl_status) {
                    if (target_matched) {
                        buf.set_str(0, "st", "1"); // GOOD_REPLICA
                    } else {
                        buf.set_str(0, "st", "0"); // STALE_REPLICA
                    }
                    modified = true;
                }

                if (target_matched) {
                    for (const auto& [kw, val] : updates) {
                        rodsLog(LOG_DEBUG, "L3_CATALOG: Applying update to replica rn=%u: kw='%s' val='%s'", rn, kw.c_str(), val.c_str());
                        if (kw == "dataModify" || kw == "modify_ts" || kw == "DATA_MODIFY_TIME") {
                            buf.set_str(0, "mt", val);
                            modified = true;
                        } else if (kw == "chksum" || kw == "data_checksum" || kw == "DATA_CHECKSUM") {
                            buf.set_str(0, "cs", val);
                            modified = true;
                        } else if (kw == "filePath" || kw == "data_path" || kw == "DATA_PATH") {
                            buf.set_str(0, "p", val);
                            modified = true;
                        } else if (kw == "rescHier" || kw == "resc_hier" || kw == "DATA_RESC_HIER") {
                            buf.set_str(0, "rh", val);
                            modified = true;
                        } else if (kw == "rescId" || kw == "resc_id" || kw == "DATA_RESC_ID") {
                            try {
                                buf.set_i64(0, "rid", std::stoll(val));
                                modified = true;
                            } catch (...) {}
                        } else if (kw == "replStatus" || kw == "data_is_dirty" || kw == "DATA_REPL_STATUS") {
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
                    rodsLog(LOG_DEBUG, "L3_CATALOG: Putting modified node for replica rn=%u (SID: %016llx)", rn, (unsigned long long)rid);
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
                                    lite3cpp::Buffer buf(payload);
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
            {
                std::lock_guard<std::mutex> lock(s_reg_cache_mu);
                s_coll_path_cache[sid] = coll.name;
            }
            {
                std::lock_guard<std::mutex> lock(s_path_cache_mu);
                s_coll_name_cache[coll.name] = sid;
            }

            snowflake_id_t psid = 0;
            if (coll.parent_id != 0) {
                psid = make_id(EntityType::Collection, coll.parent_id);
            } else if (!parent_name.empty() && coll.name != "/") {
                psid = resolve_id_from_index(EntityType::Collection, "n", parent_name);
            }

            if (psid != 0 && psid != sid) {
                #ifdef IRODS_SERVER
                rodsLog(LOG_NOTICE, "L3_CATALOG: Creating CONTAINS edge (Coll-to-Coll): %016llx -- CONTAINS --> %016llx", (unsigned long long)psid, (unsigned long long)sid);
                #endif
                add_edge(psid, "CONTAINS", 1.0, sid);
            } else if (psid == 0) {
                snowflake_id_t zid = get_zone_id(coll.owner_zone);
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
                lite3cpp::Buffer buf(payload);
                buf.set_str(0, "n", new_prefix);
                buf.set_str(0, "pn", new_parent_path);
                buf.set_str(0, "mt", std::string(time_buf));
                
                client_->put_node_async(local_cluster_id_, coll_sid, buf.move_to_string()).get();
                del_index(EntityType::Collection, "n", old_prefix);
                add_index(EntityType::Collection, "n", new_prefix, coll_sid);
                {
                    std::lock_guard<std::mutex> lock(s_path_cache_mu);
                    s_coll_name_cache.erase(old_prefix);
                    s_coll_name_cache[new_prefix] = coll_sid;
                }
            }

            auto children = client_->get_neighbors_async(local_cluster_id_, coll_sid, "CONTAINS", 0.0).get();
            for (snowflake_id_t child_sid : children) {
                std::string ch_payload = client_->get_node_payload_async(local_cluster_id_, child_sid).get();
                if (ch_payload.empty()) continue;
                lite3cpp::Buffer ch_buf(ch_payload);
                std::string ch_type = safe_get_str(ch_buf, 0, "t");
                std::string ch_entity_type = safe_get_str(ch_buf, 0, "entity_type");
                bool is_coll = (ch_entity_type == "collection" || (ch_entity_type.empty() && ch_type == "collection"));
                if (is_coll) {
                    std::string old_ch_name = safe_get_str(ch_buf, 0, "n");
                    std::string sub = (old_ch_name.size() >= old_prefix.size()) ? old_ch_name.substr(old_prefix.size()) : "";
                    std::string new_ch_name = new_prefix + sub;
                    update_collection_subtree(child_sid, old_ch_name, new_ch_name, new_prefix);
                } else {
                    std::string old_do_path = safe_get_str(ch_buf, 0, "p");
                    std::string do_name = safe_get_str(ch_buf, 0, "n");
                    std::string new_do_path = (new_prefix == "/" ? "/" : new_prefix + "/") + do_name;
                    std::string old_pn = safe_get_str(ch_buf, 0, "pn");
                    if (old_pn.empty()) {
                        old_pn = old_prefix;
                    }
                    
                    ch_buf.set_str(0, "p", new_do_path);
                    ch_buf.set_str(0, "pn", new_prefix);
                    ch_buf.set_str(0, "mt", std::string(time_buf));
                    
                    client_->put_node_async(local_cluster_id_, child_sid, ch_buf.move_to_string()).get();
                    if (!old_do_path.empty()) {
                        del_index(EntityType::DataObject, "path", old_do_path);
                    }
                    add_index(EntityType::DataObject, "path", new_do_path, child_sid);

                    char id_hex[17];
                    std::snprintf(id_hex, sizeof(id_hex), "%016llx", (unsigned long long)child_sid);
                    if (!old_pn.empty()) {
                        client_->del_raw_async(local_cluster_id_, get_idx_key(EntityType::DataObject, "pn", old_pn) + ":" + id_hex);
                    }
                    client_->put_raw_async(local_cluster_id_, get_idx_key(EntityType::DataObject, "pn", new_prefix) + ":" + id_hex, id_hex);
                }
            }
        }

        void get_collection_subtree_ids(snowflake_id_t coll_sid, std::vector<snowflake_id_t>& out_ids, std::unordered_set<snowflake_id_t>& visited) {
            if (!visited.insert(coll_sid).second) return;
            out_ids.push_back(coll_sid);

            // Fast path: prefix scan over collection name index
            try {
                std::string payload = client_->get_node_payload_async(local_cluster_id_, coll_sid).get();
                if (!payload.empty()) {
                    lite3cpp::Buffer buf(reinterpret_cast<const uint8_t*>(payload.data()), payload.size());
                    std::string coll_name = safe_get_str(buf, 0, "n");
                    if (!coll_name.empty()) {
                        std::string prefix = get_idx_key(EntityType::Collection, "n", coll_name == "/" ? "/" : coll_name + "/");
                        auto entries = client_->get_prefix_entries_async(local_cluster_id_, prefix).get();
                        for (const auto& [k, v] : entries) {
                            if (!v.empty()) {
                                try {
                                    snowflake_id_t child_sid = std::stoull(v, nullptr, 16);
                                    if (visited.insert(child_sid).second) {
                                        out_ids.push_back(child_sid);
                                    }
                                } catch (...) {}
                            }
                        }
                        return; // Fast-path completed successfully! Return immediately.
                    }
                }
            } catch (...) {
                // Fall back to graph traversal only upon error
            }

            // Fallback path: graph traversal
            try {
                auto children = client_->get_neighbors_async(local_cluster_id_, coll_sid, "CONTAINS", 0.0).get();
                for (snowflake_id_t child_sid : children) {
                    std::string ch_payload = client_->get_node_payload_async(local_cluster_id_, child_sid).get();
                    if (ch_payload.empty()) continue;
                    lite3cpp::Buffer ch_buf(reinterpret_cast<const uint8_t*>(ch_payload.data()), ch_payload.size());
                    std::string ch_type = safe_get_str(ch_buf, 0, "t");
                    std::string ch_entity_type = safe_get_str(ch_buf, 0, "entity_type");
                    bool is_coll = (ch_entity_type == "collection" || (ch_entity_type.empty() && ch_type == "collection"));
                    if (is_coll) {
                        get_collection_subtree_ids(child_sid, out_ids, visited);
                    }
                }
            } catch (...) {}
        }

        irods::error get_collection_subtree_ids(snowflake_id_t coll_sid, std::vector<snowflake_id_t>& out_ids) {
            std::unordered_set<snowflake_id_t> visited;
            get_collection_subtree_ids(coll_sid, out_ids, visited);
            return SUCCESS();
        }

        irods::error get_child_collection_ids(snowflake_id_t parent_sid, std::string_view parent_path, std::vector<snowflake_id_t>& out_ids) {
            std::string path_str(parent_path);
            if (path_str.empty()) {
                std::string payload = client_->get_node_payload_async(local_cluster_id_, parent_sid).get();
                if (!payload.empty()) {
                    lite3cpp::Buffer buf(payload);
                    path_str = safe_get_str(buf, 0, "n");
                }
            }
            if (!path_str.empty()) {
                try {
                    std::string prefix = get_idx_key(EntityType::Collection, "n", path_str == "/" ? "/" : path_str + "/");
                    size_t prefix_len = prefix.size();
                    auto entries = client_->get_prefix_entries_async(local_cluster_id_, prefix).get();
                    for (const auto& [k, v] : entries) {
                        if (k.size() > prefix_len) {
                            std::string_view remainder(k.data() + prefix_len, k.size() - prefix_len);
                            if (remainder.find('/') == std::string_view::npos && !v.empty()) {
                                try {
                                    out_ids.push_back(std::stoull(v, nullptr, 16));
                                } catch (...) {}
                            }
                        }
                    }
                    return SUCCESS();
                } catch (...) {}
            }

            // Fallback path: graph traversal
            try {
                auto children = client_->get_neighbors_async(local_cluster_id_, parent_sid, "CONTAINS", 0.0).get();
                for (snowflake_id_t child_sid : children) {
                    std::string ch_payload = client_->get_node_payload_async(local_cluster_id_, child_sid).get();
                    if (ch_payload.empty()) continue;
                    lite3cpp::Buffer ch_buf(ch_payload);
                    std::string ch_type = safe_get_str(ch_buf, 0, "t");
                    std::string ch_entity_type = safe_get_str(ch_buf, 0, "entity_type");
                    if (ch_entity_type == "collection" || (ch_entity_type.empty() && ch_type == "collection")) {
                        out_ids.push_back(child_sid);
                    }
                }
            } catch (...) {}
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
            {
                std::lock_guard<std::mutex> lock(s_reg_cache_mu);
                s_coll_path_cache.clear();
            }
            {
                std::lock_guard<std::mutex> lock(s_path_cache_mu);
                s_coll_name_cache.erase(std::string(old_name));
            }
            return SUCCESS(); 
        }

        irods::error rename_collection_by_id(coll_id_t coll_id, std::string_view new_name) {
            snowflake_id_t cid = make_id(EntityType::Collection, coll_id);
            std::string payload = client_->get_node_payload_async(local_cluster_id_, cid).get();
            if (payload.empty()) {
                return ERROR(CAT_UNKNOWN_COLLECTION, "Collection not found");
            }
            lite3cpp::Buffer buf(payload);
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
            {
                std::lock_guard<std::mutex> lock(s_reg_cache_mu);
                s_coll_path_cache.clear();
            }
            {
                std::lock_guard<std::mutex> lock(s_path_cache_mu);
                s_coll_name_cache.erase(old_coll_name);
            }
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

            lite3cpp::Buffer buf(payload);
            lite3cpp::Buffer tbuf(target_payload);

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
            {
                std::lock_guard<std::mutex> lock(s_reg_cache_mu);
                s_coll_path_cache.clear();
            }
            return SUCCESS();
        }

        irods::error rename_object(uint64_t obj_id, std::string_view new_name) {
            snowflake_id_t cid = make_id(EntityType::Collection, obj_id);
            std::string c_payload = client_->get_node_payload_async(local_cluster_id_, cid).get();
            snowflake_id_t did = make_id(EntityType::DataObject, obj_id);
            std::string d_payload = client_->get_node_payload_async(local_cluster_id_, did).get();

            if (!c_payload.empty() && !d_payload.empty()) {
                lite3cpp::Buffer cbuf(c_payload);
                lite3cpp::Buffer dbuf(d_payload);
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
            snowflake_id_t did = make_id(EntityType::DataObject, obj_id);
            auto f_c = client_->get_node_payload_async(local_cluster_id_, cid);
            auto f_d = client_->get_node_payload_async(local_cluster_id_, did);
            std::string c_payload = f_c.get();
            std::string d_payload = f_d.get();

            if (!c_payload.empty() && !d_payload.empty()) {
                lite3cpp::Buffer cbuf(c_payload);
                lite3cpp::Buffer dbuf(d_payload);
                snowflake_id_t target_cid = make_id(EntityType::Collection, target_coll_id);
                std::string target_payload = client_->get_node_payload_async(local_cluster_id_, target_cid).get();
                if (!target_payload.empty()) {
                    lite3cpp::Buffer tbuf(target_payload);
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
        irods::error is_collection_empty(coll_id_t coll_id, bool& is_empty) {
            is_empty = true;
            snowflake_id_t sid = make_id(EntityType::Collection, coll_id);
            std::string payload = client_->get_node_payload_async(local_cluster_id_, sid).get();
            if (payload.empty()) {
                std::string direct_payload = client_->get_node_payload_async(local_cluster_id_, coll_id).get();
                if (!direct_payload.empty()) {
                    sid = coll_id;
                    payload = direct_payload;
                } else {
                    return ERROR(CAT_UNKNOWN_COLLECTION, "Collection not found");
                }
            }
            auto children = client_->get_neighbors_async(local_cluster_id_, sid, "CONTAINS", 0.0).get();
            for (auto cid : children) {
                if (cid == sid) continue;
                std::string cpayload = client_->get_node_payload_async(local_cluster_id_, cid).get();
                if (!cpayload.empty()) {
                    try {
                        lite3cpp::Buffer cbuf(reinterpret_cast<const uint8_t*>(cpayload.data()), cpayload.size());
                        if (cbuf.get_bool(0, "tombstone")) continue;
                    } catch (...) {}
                    is_empty = false;
                    return SUCCESS();
                }
            }
            return SUCCESS();
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
            rodsLog(LOG_DEBUG, "L3_CATALOG: Deleting Collection %llu (SID: %016llx)", (unsigned long long)coll_id, (unsigned long long)sid);
            #endif

            bool empty = true;
            auto empty_res = is_collection_empty(coll_id, empty);
            if (!empty_res.ok()) {
                return empty_res;
            }
            if (!empty) {
                return ERROR(CAT_COLLECTION_NOT_EMPTY, "Collection is not empty");
            }

            l3kvg::MutationBatch batch;

            // Delete path index and edges
            std::string deleted_coll_path;
            if (!payload.empty()) {
                try {
                    lite3cpp::Buffer buf(reinterpret_cast<const uint8_t*>(payload.data()), payload.size());
                    std::string path = safe_get_str(buf, 0, "n");
                    deleted_coll_path = path;
                    if (!path.empty()) {
                        batch.del_raw(get_idx_key(EntityType::Collection, "n", path));
                    }
                    std::string id_str = safe_get_str(buf, 0, "id");
                    if (!id_str.empty()) {
                        batch.del_raw(get_idx_key(EntityType::Collection, "id", id_str));
                    } else if (coll_id != 0) {
                        batch.del_raw(get_idx_key(EntityType::Collection, "id", std::to_string(coll_id)));
                    }
                } catch (...) {}
            }

            // Pipelined parallel query futures for edges
            auto f_coll = client_->get_in_neighbors_async(local_cluster_id_, sid, "CONTAINS");
            auto f_children = client_->get_neighbors_async(local_cluster_id_, sid, "CONTAINS", 0.0);
            auto f_owns = client_->get_in_neighbors_async(local_cluster_id_, sid, "OWNS");
            auto f_access = client_->get_in_neighbors_async(local_cluster_id_, sid, "FOR_OBJECT");
            auto f_avus = client_->get_neighbors_async(local_cluster_id_, sid, "ANNOTATED_WITH", 0.0);

            // Delete incoming edges (CONTAINS, OWNS)
            auto collections = f_coll.get();
            for (auto cid : collections) {
                batch.del_edge(cid, "CONTAINS", 1.0, sid);
            }

            // Delete outgoing edges
            auto children = f_children.get();
            for (auto child_id : children) {
                batch.del_edge(sid, "CONTAINS", 1.0, child_id);
            }
            auto owners = f_owns.get();
            for (auto oid : owners) {
                batch.del_edge(oid, "OWNS", 1.0, sid);
            }
            auto accesses = f_access.get();
            std::vector<std::pair<snowflake_id_t, std::future<std::vector<snowflake_id_t>>>> f_access_uids;
            f_access_uids.reserve(accesses.size());
            for (auto aid : accesses) {
                f_access_uids.emplace_back(aid, client_->get_in_neighbors_async(local_cluster_id_, aid, "HAS_ACCESS"));
            }

            auto avus = f_avus.get();
            std::vector<std::pair<snowflake_id_t, std::future<std::vector<snowflake_id_t>>>> f_avu_refs;
            f_avu_refs.reserve(avus.size());
            for (auto aid : avus) {
                f_avu_refs.emplace_back(aid, client_->get_in_neighbors_async(local_cluster_id_, aid, "ANNOTATED_WITH"));
            }

            for (auto& [aid, fut] : f_access_uids) {
                auto uids = fut.get();
                for (auto uid : uids) {
                    batch.del_edge(uid, "HAS_ACCESS", 1.0, aid);
                }
                batch.del_edge(aid, "FOR_OBJECT", 1.0, sid);
                batch.del_node(aid);
            }
            for (auto& [aid, fut] : f_avu_refs) {
                batch.del_edge(sid, "ANNOTATED_WITH", 1.0, aid);
                auto refs = fut.get();
                if (refs.size() <= 1) {
                    batch.del_node(aid);
                }
            }
            batch.del_node(sid);
            client_->execute_batch_async(local_cluster_id_, batch).get();

            {
                std::lock_guard<std::mutex> lock(s_reg_cache_mu);
                s_coll_path_cache.erase(sid);
            }
            if (!deleted_coll_path.empty()) {
                std::lock_guard<std::mutex> lock(s_path_cache_mu);
                s_coll_name_cache.erase(deleted_coll_path);
            }
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
            lite3cpp::Buffer buf(payload);
            
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
            buf.set_str(0, "entity_type", "resource");
            buf.set_str(0, "ch", "");
            buf.set_str(0, "p", "");
            buf.set_str(0, "pc", "");
            client_->put_node_async(local_cluster_id_, sid, buf.move_to_string()).get();
            add_index(EntityType::Resource, "n", resc.name, sid);
            add_index(EntityType::Resource, "id", std::to_string(resc.id), sid);
            snowflake_id_t zid = get_zone_id();
            add_edge(zid, "HAS_RESC", 1.0, sid);
            out_id = sid; return SUCCESS();
        }
        irods::error modify_resource(snowflake_id_t sid, std::string_view prop, std::string_view value) { 
            std::string payload = client_->get_node_payload_async(local_cluster_id_, sid).get();
            if (!payload.empty()) {
                 lite3cpp::Buffer buf(payload);
                 
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
                    lite3cpp::Buffer buf(payload);
                    std::string name = safe_get_str(buf, 0, "n");
                    del_index(EntityType::Resource, "n", name);
                    std::string id_str = safe_get_str(buf, 0, "id");
                    if (!id_str.empty()) del_index(EntityType::Resource, "id", id_str);
                } catch (...) {}
            }
            snowflake_id_t zid = get_zone_id();
            del_edge(zid, "HAS_RESC", 1.0, sid);
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
            snowflake_id_t sid = resolve_user(name);
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
                        lite3cpp::Buffer buf(p_payload);
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
            if (!pid || !cid) return ERROR(CAT_INVALID_RESOURCE, "Parent or child resource not found");
            
            add_edge(pid, "HAS_CHILD", 1.0, cid);

            std::string parent_id_str;
            std::string p_payload = client_->get_node_payload_async(local_cluster_id_, pid).get();
            if (!p_payload.empty()) {
                lite3cpp::Buffer p_buf(p_payload);
                parent_id_str = safe_get_str(p_buf, 0, "id");
                
                std::string cur_children = safe_get_str(p_buf, 0, "ch");
                irods::children_parser parser;
                if (!cur_children.empty()) {
                    parser.set_string(cur_children);
                }
                parser.add_child(std::string(child_name), std::string(context));
                std::string new_children;
                parser.str(new_children);
                p_buf.set_str(0, "ch", new_children);
                client_->put_node_async(local_cluster_id_, pid, p_buf.move_to_string()).get();
            }

            std::string c_payload = client_->get_node_payload_async(local_cluster_id_, cid).get();
            if (!c_payload.empty()) {
                lite3cpp::Buffer c_buf(c_payload);
                if (!parent_id_str.empty()) {
                    c_buf.set_str(0, "p", parent_id_str);
                }
                c_buf.set_str(0, "pc", std::string(context));
                client_->put_node_async(local_cluster_id_, cid, c_buf.move_to_string()).get();
            }

            return SUCCESS(); 
        }
        irods::error remove_child_resource(std::string_view parent_name, std::string_view child_name) { 
            snowflake_id_t pid = resolve_id_from_index(EntityType::Resource, "n", parent_name);
            snowflake_id_t cid = resolve_id_from_index(EntityType::Resource, "n", child_name);
            if (!pid || !cid) return ERROR(CAT_INVALID_RESOURCE, "Parent or child resource not found");
            
            std::string edge_key = std::string(l3kvg::KeyBuilder::edge_out_key(pid, "HAS_CHILD", 1.0, cid));
            client_->del_edge_async(local_cluster_id_, edge_key).get();

            std::string p_payload = client_->get_node_payload_async(local_cluster_id_, pid).get();
            if (!p_payload.empty()) {
                lite3cpp::Buffer p_buf(p_payload);
                std::string cur_children = safe_get_str(p_buf, 0, "ch");
                irods::children_parser parser;
                if (!cur_children.empty()) {
                    parser.set_string(cur_children);
                    parser.remove_child(std::string(child_name));
                    std::string new_children;
                    parser.str(new_children);
                    p_buf.set_str(0, "ch", new_children);
                    client_->put_node_async(local_cluster_id_, pid, p_buf.move_to_string()).get();
                }
            }

            std::string c_payload = client_->get_node_payload_async(local_cluster_id_, cid).get();
            if (!c_payload.empty()) {
                lite3cpp::Buffer c_buf(c_payload);
                c_buf.set_str(0, "p", "");
                c_buf.set_str(0, "pc", "");
                client_->put_node_async(local_cluster_id_, cid, c_buf.move_to_string()).get();
            }

            return SUCCESS(); 
        }
        irods::error update_resource_object_count(resc_id_t resc_id, int delta) { 
            snowflake_id_t sid = make_id(EntityType::Resource, resc_id);
            std::string payload = client_->get_node_payload_async(local_cluster_id_, sid).get();
            if (!payload.empty()) {
                 lite3cpp::Buffer buf(payload);
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
            std::string clean_name = usr.name;
            std::string clean_zone = usr.zone;
            auto hpos = clean_name.find('#');
            if (hpos != std::string::npos) {
                clean_zone = clean_name.substr(hpos + 1);
                clean_name = clean_name.substr(0, hpos);
            }
            if (clean_zone.empty()) {
                clean_zone = local_zone_name_;
            }

            snowflake_id_t sid = make_id(EntityType::User, usr.id);
            snowflake_id_t existing_sid = resolve_id_from_index(EntityType::User, "nz", clean_name + "#" + clean_zone);
            if (!existing_sid && (clean_zone == local_zone_name_)) {
                existing_sid = resolve_id_from_index(EntityType::User, "n", clean_name);
            }
            if (existing_sid && existing_sid != sid) {
                return ERROR(CATALOG_ALREADY_HAS_ITEM_BY_THAT_NAME, "User already exists");
            }
            int priv = (usr.type == "rodsadmin" ? 5 : 1);
            #ifdef IRODS_SERVER
            rodsLog(LOG_NOTICE, "L3_CATALOG: Registering User [%s#%s] type [%s] id [%llu] priv [%d]", clean_name.c_str(), clean_zone.c_str(), usr.type.c_str(), (unsigned long long)usr.id, priv);
            #endif
            char time_buf[64];
            snprintf(time_buf, sizeof(time_buf), "%011ld", time(nullptr));

            lite3cpp::Buffer buf; buf.init_object(); 
            buf.set_str(0, "n", clean_name);
            buf.set_str(0, "t", usr.type);
            buf.set_str(0, "z", clean_zone);
            buf.set_i64(0, "p", priv);
            buf.set_i64(0, "id", static_cast<int64_t>(usr.id));
            buf.set_str(0, "c", "");
            buf.set_str(0, "i", "");
            buf.set_str(0, "ct", time_buf);
            buf.set_str(0, "mt", time_buf);
            client_->put_node_async(local_cluster_id_, sid, buf.move_to_string()).get();
            add_index(EntityType::User, "nz", clean_name + "#" + clean_zone, sid);
            if (clean_zone == local_zone_name_) {
                add_index(EntityType::User, "n", clean_name, sid);
            }
            add_index(EntityType::User, "id", std::to_string(usr.id), sid);
            snowflake_id_t zid = get_zone_id(clean_zone);
            if (!zid) zid = get_zone_id(local_zone_name_);
            if (zid) {
                add_edge(zid, "HAS_USER", 1.0, sid);
            }
            add_edge(sid, "MEMBER_OF", 1.0, sid);
            if (usr.type != "rodsgroup") {
                snowflake_id_t gid_pub = resolve_id_from_index(EntityType::User, "nz", "public#" + clean_zone);
                if (!gid_pub) gid_pub = resolve_id_from_index(EntityType::User, "nz", "public#" + local_zone_name_);
                if (!gid_pub) gid_pub = resolve_id_from_index(EntityType::User, "n", "public");
                if (!gid_pub) gid_pub = make_id(EntityType::User, 2);
                add_edge(sid, "MEMBER_OF", 1.0, gid_pub);
            }
            invalidate_user_cache(sid);
            out_id = usr.id; return SUCCESS();
        }
        irods::error delete_user(std::string_view user_name, std::string_view zone = "") { 
            std::string clean_name(user_name);
            std::string clean_zone(zone);
            auto hpos = clean_name.find('#');
            if (hpos != std::string::npos) {
                clean_zone = clean_name.substr(hpos + 1);
                clean_name = clean_name.substr(0, hpos);
            }
            if (clean_zone.empty()) {
                clean_zone = local_zone_name_;
            }

            snowflake_id_t uid = resolve_user(clean_name, clean_zone);
            if (!uid) {
                return ERROR(CAT_INVALID_USER, "User not found");
            }
            invalidate_user_cache(uid);
            snowflake_id_t zid = get_zone_id(clean_zone);
            if (zid) {
                del_edge(zid, "HAS_USER", 1.0, uid);
            }
            del_edge(uid, "MEMBER_OF", 1.0, uid);
            del_index(EntityType::User, "nz", clean_name + "#" + clean_zone);
            if (clean_zone == local_zone_name_) {
                del_index(EntityType::User, "n", clean_name);
                del_index(EntityType::User, "n", user_name);
            }
            std::string payload = client_->get_node_payload_async(local_cluster_id_, uid).get();
            if (!payload.empty()) {
                try {
                    lite3cpp::Buffer buf(payload);
                    std::string id_str = safe_get_str(buf, 0, "id");
                    if (!id_str.empty()) del_index(EntityType::User, "id", id_str);
                } catch (...) {}
            }
            auto groups = client_->get_neighbors_async(local_cluster_id_, uid, "MEMBER_OF", 0.0).get();
            for (auto g : groups) {
                del_edge(uid, "MEMBER_OF", 1.0, g);
            }
            auto members = client_->get_in_neighbors_async(local_cluster_id_, uid, "MEMBER_OF").get();
            for (auto m : members) {
                del_edge(m, "MEMBER_OF", 1.0, uid);
                invalidate_user_cache(m);
            }
            auto aids = client_->get_neighbors_async(local_cluster_id_, uid, "HAS_ACCESS", 0.0).get();
            for (auto aid : aids) {
                del_edge(uid, "HAS_ACCESS", 1.0, aid);
                std::string aid_payload = client_->get_node_payload_async(local_cluster_id_, aid).get();
                if (!aid_payload.empty()) {
                    try {
                        lite3cpp::Buffer abuf(aid_payload);
                        int64_t owner_uid = abuf.get_i64(0, "uid");
                        if (static_cast<uint64_t>(owner_uid) == uid) {
                            for (auto m : members) {
                                del_edge(m, "HAS_ACCESS", 1.0, aid);
                            }
                            auto targets = client_->get_neighbors_async(local_cluster_id_, aid, "FOR_OBJECT", 0.0).get();
                            for (auto tid : targets) {
                                del_edge(aid, "FOR_OBJECT", 1.0, tid);
                            }
                            client_->del_node_async(local_cluster_id_, aid).get();
                        }
                    } catch (...) {}
                }
            }
            client_->del_node_async(local_cluster_id_, uid).get();
            return SUCCESS(); 
        }
        irods::error modify_user(std::string_view user_name, std::string_view prop, std::string_view value, std::string_view zone = "") { 
            snowflake_id_t uid = resolve_user(user_name, zone);
            if (!uid) return ERROR(CAT_INVALID_USER, "User not found");
            invalidate_user_cache(uid);
            
            std::string payload = client_->get_node_payload_async(local_cluster_id_, uid).get();
            if (!payload.empty()) {
                 lite3cpp::Buffer buf(payload);
                 if (prop == "type") {
                     buf.set_i64(0, "p", (value == "rodsadmin" ? 5 : 1));
                     buf.set_str(0, "t", std::string(value));
                 } else if (prop == "password") {
                     buf.set_str(0, "pw", value);
                 } else if (prop == "comment") {
                     buf.set_str(0, "c", std::string(value));
                 } else if (prop == "info") {
                     buf.set_str(0, "i", std::string(value));
                     buf.set_str(0, "info", std::string(value));
                 }
                 char time_buf[64];
                 snprintf(time_buf, sizeof(time_buf), "%011ld", time(nullptr));
                 buf.set_str(0, "mt", time_buf);
                 client_->put_node_async(local_cluster_id_, uid, buf.move_to_string()).get();
            }
            return SUCCESS(); 
        }
        irods::error check_auth(std::string_view user_name, std::string_view zone, int& user_priv) {
            snowflake_id_t uid = resolve_user(user_name, zone);
            if (!uid) return ERROR(-1, "User not found");
            auto fut = client_->get_node_payload_async(local_cluster_id_, uid);
            std::string payload = fut.get();
            if (payload.empty()) return ERROR(-1, "User node missing");
            try {
                lite3cpp::Buffer buf(payload);
                user_priv = static_cast<int>(buf.get_i64(0, "p"));
                #ifdef IRODS_SERVER
                rodsLog(LOG_NOTICE, "L3_CATALOG: check_auth user=[%s] priv=[%d]", user_name.data(), user_priv);
                #endif
                return SUCCESS();
            } catch (...) { return ERROR(-1, "Failed to parse priv level"); }
        }
        irods::error get_user_password_and_priv(std::string_view user_name, std::string_view zone, std::string& out_pw, int& out_priv) {
            snowflake_id_t uid = resolve_user(user_name, zone);
            if (!uid) return ERROR(-1, "User not found");
            auto fut = client_->get_node_payload_async(local_cluster_id_, uid);
            std::string payload = fut.get();
            if (payload.empty()) return ERROR(-1, "User node missing");
            try {
                lite3cpp::Buffer buf(payload);
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
            snowflake_id_t uid = resolve_user(username, zone);
            if (!uid) return SUCCESS();

            std::string payload = client_->get_node_payload_async(local_cluster_id_, uid).get();
            if (!payload.empty()) {
                try {
                    lite3cpp::Buffer buf(payload);
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
            snowflake_id_t uid = resolve_user(user_name, zone);
            snowflake_id_t gid = resolve_user(group_name, zone);
            if (!gid) {
                gid = resolve_user(group_name, local_zone_name_);
            }
            if (!uid || !gid) return ERROR(-1, "User or group not found");
            invalidate_user_cache(uid);
            invalidate_user_cache(gid);
            add_edge(uid, "MEMBER_OF", 1.0, gid);
            auto aids = client_->get_neighbors_async(local_cluster_id_, gid, "HAS_ACCESS", 0.0).get();
            for (auto aid : aids) {
                add_edge(uid, "HAS_ACCESS", 1.0, aid);
            }
            return SUCCESS(); 
        }
        irods::error remove_user_from_group(std::string_view user_name, std::string_view zone, std::string_view group_name) { 
            snowflake_id_t uid = resolve_user(user_name, zone);
            snowflake_id_t gid = resolve_user(group_name, zone);
            if (!gid) {
                gid = resolve_user(group_name, local_zone_name_);
            }
            if (!uid || !gid) return ERROR(-1, "User or group not found");
            invalidate_user_cache(uid);
            invalidate_user_cache(gid);
            
            del_edge(uid, "MEMBER_OF", 1.0, gid);
            auto aids = client_->get_neighbors_async(local_cluster_id_, gid, "HAS_ACCESS", 0.0).get();
            for (auto aid : aids) {
                del_edge(uid, "HAS_ACCESS", 1.0, aid);
            }
            return SUCCESS(); 
        }

        // --- ACLs ---
        irods::error set_access(std::string_view user_name, std::string_view zone, std::string_view target_path, std::string_view level, bool recursive) { 
            snowflake_id_t uid = resolve_user(user_name, zone);
            if (!uid) return ERROR(-1, "User not found");
            
            // Resolve target path (could be data or collection)
            snowflake_id_t tid = 0;
            EntityType type;
            if (!resolve_path(target_path, tid, type).ok()) {
                return ERROR(-1, "Target path not found: " + std::string(target_path));
            }

            auto members = client_->get_in_neighbors_async(local_cluster_id_, uid, "MEMBER_OF").get();

            auto apply_node_access = [&](snowflake_id_t target_id) {
                std::string aid_uuid = std::to_string(uid) + ":" + std::to_string(target_id);
                snowflake_id_t aid = SnowflakeID::create(local_cluster_id_, aid_uuid);

                if (level == "null" || level == "admin:null") {
                    del_edge(uid, "HAS_ACCESS", 1.0, aid);
                    for (auto mid : members) {
                        del_edge(mid, "HAS_ACCESS", 1.0, aid);
                    }
                    del_edge(aid, "FOR_OBJECT", 1.0, target_id);
                    client_->del_node_async(local_cluster_id_, aid).get();
                    return;
                }
                
                lite3cpp::Buffer buf; buf.init_object(); 
                buf.set_str(0, "l", std::string(level));
                buf.set_str(0, "t", "access_type");
                buf.set_str(0, "entity_type", "access");
                buf.set_str(0, "u", std::string(user_name));
                buf.set_str(0, "z", std::string(zone.empty() ? local_zone_name_ : zone));
                buf.set_i64(0, "uid", static_cast<int64_t>(uid));
                client_->put_node_async(local_cluster_id_, aid, buf.move_to_string()).get();
                
                add_edge(uid, "HAS_ACCESS", 1.0, aid);
                for (auto mid : members) {
                    add_edge(mid, "HAS_ACCESS", 1.0, aid);
                }
                add_edge(aid, "FOR_OBJECT", 1.0, target_id);
            };

            apply_node_access(tid);

            if (recursive && type == EntityType::Collection) {
                std::vector<snowflake_id_t> queue = {tid};
                std::unordered_set<snowflake_id_t> visited = {tid};
                size_t head = 0;
                while (head < queue.size()) {
                    snowflake_id_t curr = queue[head++];
                    auto children = client_->get_neighbors_async(local_cluster_id_, curr, "CONTAINS", 0.0).get();
                    for (snowflake_id_t child_sid : children) {
                        if (visited.insert(child_sid).second) {
                            apply_node_access(child_sid);
                            std::string ch_payload = client_->get_node_payload_async(local_cluster_id_, child_sid).get();
                            if (!ch_payload.empty()) {
                                lite3cpp::Buffer ch_buf(ch_payload);
                                std::string ch_type = safe_get_str(ch_buf, 0, "t");
                                std::string ch_entity_type = safe_get_str(ch_buf, 0, "entity_type");
                                bool is_coll = (ch_entity_type == "collection" || (ch_entity_type.empty() && ch_type == "collection"));
                                if (is_coll) {
                                    queue.push_back(child_sid);
                                }
                            }
                        }
                    }
                }
            }
            
            return SUCCESS(); 
        }
        irods::error check_permission(snowflake_id_t user_sid, snowflake_id_t target_sid, std::string_view level_view, bool& allowed, bool check_parents = true) { 
            std::string level(level_view);
            allowed = false;
            
            #ifdef IRODS_SERVER
            rodsLog(LOG_DEBUG, "L3_CATALOG: check_permission user_sid=%016llx target_sid=%016llx level=%s check_parents=%d",
                    (unsigned long long)user_sid, (unsigned long long)target_sid, level.c_str(), check_parents ? 1 : 0);
            #endif

            // User Info & Principals Cache
            std::string user_name;
            std::string user_type;
            std::vector<snowflake_id_t> principals;

            if (user_sid != 0) {
                bool found = false;
                auto now = std::chrono::steady_clock::now();
                {
                    std::lock_guard<std::mutex> lock(s_user_cache_mu);
                    auto it = s_user_cache.find(user_sid);
                    if (it != s_user_cache.end()) {
                        if (now < it->second.expires_at) {
                            user_name = it->second.name;
                            user_type = it->second.type;
                            principals = it->second.principals;
                            found = true;
                        }
                    }
                }
                if (!found) {
                    std::string user_payload = client_->get_node_payload_async(local_cluster_id_, user_sid).get();
                    if (!user_payload.empty()) {
                        try {
                            lite3cpp::Buffer buf(reinterpret_cast<const uint8_t*>(user_payload.data()), user_payload.size());
                            user_name = safe_get_str(buf, 0, "n");
                            user_type = safe_get_str(buf, 0, "t");
                        } catch (...) {}
                    }
                    principals.push_back(user_sid);
                    auto groups = client_->get_neighbors_async(local_cluster_id_, user_sid, "MEMBER_OF", 0.0).get();
                    principals.insert(principals.end(), groups.begin(), groups.end());

                    std::lock_guard<std::mutex> lock(s_user_cache_mu);
                    s_user_cache[user_sid] = {user_name, user_type, principals, std::chrono::steady_clock::now() + std::chrono::seconds(30)};
                }
            }

            // CRUCIAL: If user_type == "rodsadmin", return allowed = true immediately!
            // With the cache in place, rodsadmin checks cost 0 network roundtrips!
            if (user_type == "rodsadmin") {
                allowed = true;
                return SUCCESS();
            }

            // Check if target exists
            std::string t_payload = client_->get_node_payload_async(local_cluster_id_, target_sid).get();
            if (t_payload.empty()) {
                if (!check_parents) {
                    #ifdef IRODS_SERVER
                    rodsLog(LOG_DEBUG, "L3_CATALOG: check_permission target %016llx NOT FOUND", (unsigned long long)target_sid);
                    #endif
                    allowed = false;
                    return ERROR(CAT_UNKNOWN_FILE, "Target not found");
                }
                #ifdef IRODS_SERVER
                rodsLog(LOG_DEBUG, "L3_CATALOG: check_permission target %016llx NOT FOUND - allowing for now", (unsigned long long)target_sid);
                #endif
                allowed = true;
                return SUCCESS(); // Target not found, let it proceed for creation
            }

            // Direct check on owner
            if (!user_name.empty()) {
                try {
                    lite3cpp::Buffer tbuf(reinterpret_cast<const uint8_t*>(t_payload.data()), t_payload.size());
                    std::string owner = safe_get_str(tbuf, 0, "o");
                    if (owner == user_name) {
                        allowed = true;
                        return SUCCESS();
                    }
                } catch (...) {}
            }

            static auto perm_rank = [](std::string_view lvl) -> int {
                if (lvl.starts_with("admin:")) lvl = lvl.substr(6);
                if (lvl == "null") return 1000;
                if (lvl == "execute") return 1010;
                if (lvl == "read_annotation") return 1020;
                if (lvl == "read_system_metadata") return 1030;
                if (lvl == "read_metadata") return 1040;
                if (lvl == "read_object" || lvl == "read") return 1050;
                if (lvl == "write_annotation") return 1060;
                if (lvl == "create_metadata") return 1070;
                if (lvl == "modify_metadata") return 1080;
                if (lvl == "delete_metadata") return 1090;
                if (lvl == "administer_object") return 1100;
                if (lvl == "create_object") return 1110;
                if (lvl == "modify_object" || lvl == "write") return 1120;
                if (lvl == "delete_object" || lvl == "delete") return 1130;
                if (lvl == "create_token") return 1140;
                if (lvl == "delete_token") return 1150;
                if (lvl == "curate") return 1160;
                if (lvl == "own") return 1200;
                return 0;
            };

            auto check_principal_access = [&](snowflake_id_t tid) -> bool {
                for (auto pid : principals) {
                    std::string aid_uuid = std::to_string(pid) + ":" + std::to_string(tid);
                    snowflake_id_t aid = SnowflakeID::create(local_cluster_id_, aid_uuid);
                    std::string payload = client_->get_node_payload_async(local_cluster_id_, aid).get();
                    if (!payload.empty()) {
                        try {
                            lite3cpp::Buffer buf(reinterpret_cast<const uint8_t*>(payload.data()), payload.size());
                            std::string actual_level = safe_get_str(buf, 0, "l");
                            if (perm_rank(actual_level) >= perm_rank(level)) return true;
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

            if (check_parents) {
                // Check parent collection if target_sid is in a collection
                snowflake_id_t curr = target_sid;
                while (curr != 0) {
                    auto parents = client_->get_in_neighbors_async(local_cluster_id_, curr, "CONTAINS").get();
                    if (parents.empty()) {
                        break;
                    }
                    curr = parents.front();
                    if (!user_name.empty()) {
                        std::string p_payload = client_->get_node_payload_async(local_cluster_id_, curr).get();
                        if (!p_payload.empty()) {
                            try {
                                lite3cpp::Buffer pbuf(p_payload);
                                std::string p_owner = safe_get_str(pbuf, 0, "o");
                                if (p_owner == user_name) {
                                    allowed = true;
                                    return SUCCESS();
                                }
                            } catch (...) {}
                        }
                    }
                    if (check_principal_access(curr)) {
                        allowed = true;
                        return SUCCESS();
                    }
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
            if (effective_seq == "R_OBJECTID") {
                std::lock_guard<std::mutex> lock(objid_mu_);
                if (objid_curr_ < objid_limit_) {
                    out_val = ++objid_curr_;
                    return SUCCESS();
                }

                uint64_t block_end = client_->atomic_incr_async(local_cluster_id_, key, 100).get();
                if (block_end == 0 || block_end < 100) {
                    objid_limit_ = 0;
                    objid_curr_ = 0;
                    return ERROR(-1, "Failed to increment sequence: " + std::string(seq_name));
                }
                if (block_end < 100000) {
                    uint64_t jump = 100000 - block_end + 100;
                    block_end = client_->atomic_incr_async(local_cluster_id_, key, jump).get();
                    if (block_end == 0 || block_end < 100) {
                        objid_limit_ = 0;
                        objid_curr_ = 0;
                        return ERROR(-1, "Failed to increment sequence jump: " + std::string(seq_name));
                    }
                }
                objid_limit_ = block_end;
                objid_curr_ = block_end - 100;
                out_val = ++objid_curr_;
                return SUCCESS();
            }

            out_val = client_->atomic_incr_async(local_cluster_id_, key, 1).get();
            if (out_val == 0) return ERROR(-1, "Failed to increment sequence: " + std::string(seq_name));
            return SUCCESS();
        }

        l3kvg::RemoteL3KVClient* get_client() const { return client_.get(); }
        uint16_t get_cluster_id() const { return local_cluster_id_; }

        irods::error resolve_path(std::string_view path, snowflake_id_t& out_id, EntityType& out_type) {
            std::string path_str(path);
            {
                std::lock_guard<std::mutex> lock(s_path_cache_mu);
                auto it = s_coll_name_cache.find(path_str);
                if (it != s_coll_name_cache.end()) {
                    out_id = it->second;
                    out_type = EntityType::Collection;
                    return SUCCESS();
                }
            }

            snowflake_id_t sid = resolve_id_from_index(EntityType::Collection, "n", path);
            if (sid) {
                {
                    std::lock_guard<std::mutex> lock(s_path_cache_mu);
                    s_coll_name_cache[path_str] = sid;
                }
                out_id = sid;
                out_type = EntityType::Collection;
                return SUCCESS();
            }
            
            sid = resolve_id_from_index(EntityType::DataObject, "path", path);
            if (sid) { out_id = sid; out_type = EntityType::DataObject; return SUCCESS(); }

            sid = resolve_id_from_index(EntityType::DataObject, "n", path);
            if (sid) { out_id = sid; out_type = EntityType::DataObject; return SUCCESS(); }

            return ERROR(-1, "Path not found: " + std::string(path));
        }

        snowflake_id_t resolve_target_entity_sid(std::string_view type, std::string_view target_id_or_name) {
            if (target_id_or_name.empty()) return 0;

            EntityType et = EntityType::DataObject;
            if (type == "-d" || type == "-D" || type == "DataObject" || type == "data") {
                et = EntityType::DataObject;
            } else if (type == "-c" || type == "-C" || type == "Collection" || type == "coll") {
                et = EntityType::Collection;
            } else if (type == "-r" || type == "-R" || type == "Resource" || type == "resc") {
                et = EntityType::Resource;
            } else if (type == "-u" || type == "-U" || type == "User" || type == "user") {
                et = EntityType::User;
            }

            bool is_all_digits = !target_id_or_name.empty() && std::all_of(target_id_or_name.begin(), target_id_or_name.end(), [](char c){ return std::isdigit(static_cast<unsigned char>(c)); });

            if (is_all_digits) {
                try {
                    uint64_t num = std::stoull(std::string(target_id_or_name));
                    snowflake_id_t from_idx = resolve_id_from_index(et, "id", target_id_or_name);
                    if (from_idx) return from_idx;
                    return make_id(et, num);
                } catch (...) {}
            }

            if (et == EntityType::DataObject) {
                snowflake_id_t sid = resolve_id_from_index(EntityType::DataObject, "path", target_id_or_name);
                if (!sid) sid = resolve_id_from_index(EntityType::DataObject, "n", target_id_or_name);
                if (!sid) {
                    EntityType out_et;
                    resolve_path(target_id_or_name, sid, out_et);
                }
                return sid;
            } else if (et == EntityType::Collection) {
                snowflake_id_t sid = 0;
                {
                    std::lock_guard<std::mutex> lock(s_path_cache_mu);
                    auto it = s_coll_name_cache.find(std::string(target_id_or_name));
                    if (it != s_coll_name_cache.end()) {
                        return it->second;
                    }
                }
                sid = resolve_id_from_index(EntityType::Collection, "n", target_id_or_name);
                if (sid) {
                    std::lock_guard<std::mutex> lock(s_path_cache_mu);
                    s_coll_name_cache[std::string(target_id_or_name)] = sid;
                    return sid;
                }
                if (!sid) {
                    EntityType out_et;
                    resolve_path(target_id_or_name, sid, out_et);
                }
                return sid;
            } else if (et == EntityType::Resource) {
                return resolve_id_from_index(EntityType::Resource, "n", target_id_or_name);
            } else if (et == EntityType::User) {
                return resolve_user(target_id_or_name);
            }

            return 0;
        }

        // --- Metadata ---
        irods::error add_avu_metadata(std::string_view type, std::string_view target_id, const avu& metadata) {
            if (metadata.attribute.empty() || metadata.value.empty()) {
                return ERROR(CAT_INVALID_ARGUMENT, "Attribute and value cannot be empty");
            }

            snowflake_id_t target_sid = resolve_target_entity_sid(type, target_id);
            if (!target_sid) {
                return ERROR(CAT_INVALID_ARGUMENT, "Target entity not found: " + std::string(target_id));
            }

            std::string local_uuid = metadata.attribute + ":" + metadata.value + ":" + metadata.units;
            snowflake_id_t aid = SnowflakeID::create(local_cluster_id_, local_uuid);

            auto current_avus = client_->get_neighbors_async(local_cluster_id_, target_sid, "ANNOTATED_WITH", 0.0).get();
            if (std::find(current_avus.begin(), current_avus.end(), aid) != current_avus.end()) {
                return ERROR(CATALOG_ALREADY_HAS_ITEM_BY_THAT_NAME, "CATALOG_ALREADY_HAS_ITEM_BY_THAT_NAME");
            }

            lite3cpp::Buffer buf; buf.init_object(); 
            buf.set_str(0, "t", "metadata");
            buf.set_str(0, "id", std::to_string(aid));
            buf.set_str(0, "a", metadata.attribute); 
            buf.set_str(0, "v", metadata.value); 
            buf.set_str(0, "u", metadata.units);
            char time_buf[32];
            snprintf(time_buf, sizeof(time_buf), "%011lld", (long long)time(nullptr));
            buf.set_str(0, "ct", time_buf);
            buf.set_str(0, "mt", time_buf);
            client_->put_node_async(local_cluster_id_, aid, buf.move_to_string()).get();

            add_edge(target_sid, "ANNOTATED_WITH", 1.0, aid);

            char tsid_hex[17];
            std::snprintf(tsid_hex, sizeof(tsid_hex), "%016llx", (unsigned long long)target_sid);
            std::string av_key = "idx:Metadata:av:" + metadata.attribute + ":" + metadata.value;
            l3kvg::MutationBatch batch;
            batch.put_raw(av_key + ":" + std::string(tsid_hex), tsid_hex);
            client_->execute_batch_async(local_cluster_id_, batch).get();

            return SUCCESS();
        }
        irods::error delete_avu_metadata(std::string_view type, std::string_view target_id, const avu& metadata, int option = 0) { 
            snowflake_id_t target_sid = resolve_target_entity_sid(type, target_id);
            if (!target_sid) {
                return ERROR(CAT_INVALID_ARGUMENT, "Target entity not found: " + std::string(target_id));
            }

            if (option == 2) {
                try {
                    snowflake_id_t aid = std::stoull(metadata.attribute);
                    del_edge(target_sid, "ANNOTATED_WITH", 1.0, aid);
                    auto refs = client_->get_in_neighbors_async(local_cluster_id_, aid, "ANNOTATED_WITH").get();
                    if (refs.empty()) {
                        client_->del_node_async(local_cluster_id_, aid).get();
                    }
                } catch (...) {}
                return SUCCESS();
            }

            std::string local_uuid = metadata.attribute + ":" + metadata.value + ":" + metadata.units;
            snowflake_id_t aid = SnowflakeID::create(local_cluster_id_, local_uuid);

            std::string edge_key = std::string(l3kvg::KeyBuilder::edge_out_key(target_sid, "ANNOTATED_WITH", 1.0, aid));
            client_->del_edge_async(local_cluster_id_, edge_key).get();
            std::string in_key = std::string(l3kvg::KeyBuilder::edge_in_key(aid, "ANNOTATED_WITH", target_sid));
            client_->del_edge_async(local_cluster_id_, in_key).get();

            char tsid_hex[17];
            std::snprintf(tsid_hex, sizeof(tsid_hex), "%016llx", (unsigned long long)target_sid);
            std::string av_key = "idx:Metadata:av:" + metadata.attribute + ":" + metadata.value;
            l3kvg::MutationBatch batch;
            batch.del_raw(av_key + ":" + std::string(tsid_hex));
            client_->execute_batch_async(local_cluster_id_, batch).get();

            auto refs = client_->get_in_neighbors_async(local_cluster_id_, aid, "ANNOTATED_WITH").get();
            if (refs.empty()) {
                client_->del_node_async(local_cluster_id_, aid).get();
            }
            return SUCCESS(); 
        }
        irods::error modify_avu_metadata(std::string_view type, std::string_view target_id, const avu& old_avu, const avu& new_avu) { 
            delete_avu_metadata(type, target_id, old_avu);
            add_avu_metadata(type, target_id, new_avu);
            return SUCCESS(); 
        }
        irods::error copy_avu_metadata(std::string_view src_type, std::string_view src_id, std::string_view dst_type, std::string_view dst_id) { 
            snowflake_id_t src_sid = resolve_target_entity_sid(src_type, src_id);
            snowflake_id_t dst_sid = resolve_target_entity_sid(dst_type, dst_id);
            if (!src_sid || !dst_sid) {
                return ERROR(CAT_INVALID_ARGUMENT, "Source or destination entity not found");
            }

            auto avu_nodes = client_->get_neighbors_async(local_cluster_id_, src_sid, "ANNOTATED_WITH", 0.0).get();
            for (auto aid : avu_nodes) {
                add_edge(dst_sid, "ANNOTATED_WITH", 1.0, aid);
                std::string payload = client_->get_node_payload_async(local_cluster_id_, aid).get();
                if (!payload.empty()) {
                    try {
                        lite3cpp::Buffer buf(payload);
                        std::string_view a = buf.get_str(0, "a");
                        std::string_view v = buf.get_str(0, "v");
                        if (!a.empty() && !v.empty()) {
                            char tsid_hex[17];
                            std::snprintf(tsid_hex, sizeof(tsid_hex), "%016llx", (unsigned long long)dst_sid);
                            std::string av_key = "idx:Metadata:av:" + std::string(a) + ":" + std::string(v);
                            l3kvg::MutationBatch copy_b;
                            copy_b.put_raw(av_key + ":" + std::string(tsid_hex), tsid_hex);
                            client_->execute_batch_async(local_cluster_id_, copy_b).get();
                        }
                    } catch (...) {}
                }
            }
            return SUCCESS(); 
        }
        irods::error set_avu_metadata(std::string_view type, std::string_view target_id, const avu& metadata) {
            if (metadata.attribute.empty() || metadata.value.empty()) {
                return ERROR(CAT_INVALID_ARGUMENT, "Attribute and value cannot be empty");
            }
            snowflake_id_t target_sid = resolve_target_entity_sid(type, target_id);
            if (!target_sid) {
                return ERROR(CAT_INVALID_ARGUMENT, "Target entity not found: " + std::string(target_id));
            }

            auto avu_nodes = client_->get_neighbors_async(local_cluster_id_, target_sid, "ANNOTATED_WITH", 0.0).get();
            for (auto aid : avu_nodes) {
                std::string payload = client_->get_node_payload_async(local_cluster_id_, aid).get();
                if (payload.empty()) continue;
                try {
                    lite3cpp::Buffer buf(payload);
                    std::string_view a = buf.get_str(0, "a");
                    if (a == metadata.attribute) {
                        std::string_view v = buf.get_str(0, "v");
                        char tsid_hex[17];
                        std::snprintf(tsid_hex, sizeof(tsid_hex), "%016llx", (unsigned long long)target_sid);
                        std::string old_av_key = "idx:Metadata:av:" + std::string(a) + ":" + std::string(v);
                        l3kvg::MutationBatch old_b;
                        old_b.del_raw(old_av_key + ":" + std::string(tsid_hex));
                        client_->execute_batch_async(local_cluster_id_, old_b).get();

                        del_edge(target_sid, "ANNOTATED_WITH", 1.0, aid);
                        auto refs = client_->get_in_neighbors_async(local_cluster_id_, aid, "ANNOTATED_WITH").get();
                        if (refs.empty()) {
                            client_->del_node_async(local_cluster_id_, aid).get();
                        }
                    }
                } catch (...) {}
            }

            return add_avu_metadata(type, target_id, metadata);
        }

        irods::error register_zone(const zone& z) {
            snowflake_id_t existing_zid = resolve_id_from_index(EntityType::Zone, "n", z.name);
            if (existing_zid) {
                return ERROR(CATALOG_ALREADY_HAS_ITEM_BY_THAT_NAME, "Zone already exists: " + z.name);
            }
            uint64_t zid_num = 0;
            get_next_sequence_value("R_ZONE_MAIN", zid_num);
            if (zid_num == 0) zid_num = 1;
            snowflake_id_t zid = make_id(EntityType::Zone, zid_num);
            lite3cpp::Buffer buf; buf.init_object(); 
            buf.set_i64(0, "id", static_cast<int64_t>(zid_num));
            buf.set_str(0, "n", z.name); 
            buf.set_str(0, "t", z.type); 
            buf.set_str(0, "c", z.connection); 
            buf.set_str(0, "m", z.comment);
            client_->put_node_async(local_cluster_id_, zid, buf.move_to_string()).get();
            add_index(EntityType::Zone, "n", z.name, zid);
            add_index(EntityType::Zone, "id", std::to_string(zid_num), zid);
            add_index(EntityType::Zone, "t", z.type, zid);

            snowflake_id_t local_zid = get_zone_id(local_zone_name_);
            if (local_zid) {
                add_edge(local_zid, "HAS_ZONE", 1.0, zid);
            }
            return SUCCESS();
        }
        irods::error modify_zone(std::string_view name, std::string_view prop, std::string_view value) { 
            snowflake_id_t zid = resolve_id_from_index(EntityType::Zone, "n", name);
            if (!zid) return ERROR(-1, "Zone not found");
            
            // In a graph we'd patch the node
            // For now, we don't have patch_str_async in RemoteL3KVClient, so we do full put
            std::string payload = client_->get_node_payload_async(local_cluster_id_, zid).get();
            if (!payload.empty()) {
                 lite3cpp::Buffer buf(payload);
                 buf.set_str(0, std::string(prop), std::string(value));
                 client_->put_node_async(local_cluster_id_, zid, buf.move_to_string()).get();
            }
            return SUCCESS(); 
        }
        irods::error delete_zone(std::string_view name) { 
            snowflake_id_t zid = resolve_id_from_index(EntityType::Zone, "n", name);
            if (zid) {
                snowflake_id_t local_zid = get_zone_id(local_zone_name_);
                if (local_zid) {
                    del_edge(local_zid, "HAS_ZONE", 1.0, zid);
                }
                del_index(EntityType::Zone, "n", name);
                std::string payload = client_->get_node_payload_async(local_cluster_id_, zid).get();
                if (!payload.empty()) {
                    try {
                        lite3cpp::Buffer buf(payload);
                        std::string id_str = safe_get_str(buf, 0, "id");
                        if (!id_str.empty()) del_index(EntityType::Zone, "id", id_str);
                        std::string type_str = safe_get_str(buf, 0, "t");
                        if (!type_str.empty()) del_index(EntityType::Zone, "t", type_str);
                    } catch (...) {}
                }
                client_->del_node_async(local_cluster_id_, zid).get();
            }
            return SUCCESS(); 
        }

        // --- Ticket Operations ---
        static std::vector<std::string> split_csv(const std::string& str) {
            std::vector<std::string> result;
            std::stringstream ss(str);
            std::string token;
            while (std::getline(ss, token, ',')) {
                size_t start = token.find_first_not_of(" \t\r\n");
                size_t end = token.find_last_not_of(" \t\r\n");
                if (start != std::string::npos && end != std::string::npos && end >= start) {
                    result.push_back(token.substr(start, end - start + 1));
                }
            }
            return result;
        }

        static std::string join_csv(const std::vector<std::string>& vec) {
            std::string result;
            for (size_t i = 0; i < vec.size(); ++i) {
                if (i > 0) result += ",";
                result += vec[i];
            }
            return result;
        }

        static char* convertHostToIp(const char* inputName) {
            if (!inputName || *inputName == '\0') return nullptr;
            static thread_local char ipAddr[INET_ADDRSTRLEN];
            struct addrinfo hint;
            memset(&hint, 0, sizeof(hint));
            hint.ai_family = AF_INET;
            struct addrinfo *p_addrinfo = nullptr;
            int status = getaddrinfo(inputName, nullptr, &hint, &p_addrinfo);
            if (status != 0 || !p_addrinfo) {
                return nullptr;
            }
            const char* res = inet_ntop(AF_INET, &(reinterpret_cast<struct sockaddr_in*>(p_addrinfo->ai_addr)->sin_addr), ipAddr, sizeof(ipAddr));
            freeaddrinfo(p_addrinfo);
            return const_cast<char*>(res);
        }

        static std::string parse_expire_string(std::string_view val) {
            if (val.empty() || val == "0") {
                return "00000000000";
            }
            std::string s(val);
            try {
                if (s.find('-') == std::string::npos && s.find('.') == std::string::npos) {
                    int64_t sec = std::stoll(s);
                    return fmt::format("{:011}", sec);
                }
            } catch (...) {}

            struct tm tm_val{};
            if (strptime(s.c_str(), "%Y-%m-%d.%H:%M:%S", &tm_val) != nullptr ||
                strptime(s.c_str(), "%Y-%m-%d", &tm_val) != nullptr) {
                time_t t = timegm(&tm_val);
                return fmt::format("{:011}", (int64_t)t);
            }
            return "00000000000";
        }

        irods::error create_ticket(uint64_t ticket_id, std::string_view ticket_string, std::string_view ticket_type, snowflake_id_t target_sid, EntityType target_type, snowflake_id_t user_sid, std::string_view user_name, std::string_view target_path = "") {
            snowflake_id_t sid = make_id(EntityType::Ticket, ticket_id);
            lite3cpp::Buffer buf(4096); buf.init_object();
            buf.set_i64(0, "id", static_cast<int64_t>(ticket_id));
            buf.set_str(0, "s", std::string(ticket_string));
            buf.set_str(0, "t", std::string(ticket_type));
            buf.set_str(0, "ot", (target_type == EntityType::DataObject ? "data" : "collection"));
            buf.set_str(0, "own", std::string(user_name));
            buf.set_str(0, "entity_type", "ticket");

            std::string tpath(target_path);
            uint64_t target_int_id = 0;
            if (target_sid) {
                std::string tpayload = client_->get_node_payload_async(local_cluster_id_, target_sid).get();
                if (!tpayload.empty()) {
                    lite3cpp::Buffer tbuf(tpayload);
                    if (tpath.empty()) {
                        tpath = safe_get_str(tbuf, 0, "p");
                        if (tpath.empty()) tpath = safe_get_str(tbuf, 0, "n");
                    }
                    target_int_id = tbuf.get_i64(0, "id");
                }
            }
            buf.set_str(0, "target_path", tpath);

            char myTime[32]{};
            getNowStr(myTime);
            buf.set_str(0, "ct", myTime);
            buf.set_str(0, "mt", myTime);

            buf.set_i64(0, "ul", 0);
            buf.set_i64(0, "uc", 0);
            buf.set_i64(0, "wfl", 10);
            buf.set_i64(0, "wfc", 0);
            buf.set_i64(0, "wbl", 0);
            buf.set_i64(0, "wbc", 0);
            buf.set_str(0, "ex", "00000000000");
            buf.set_str(0, "allowed_hosts", "");
            buf.set_str(0, "allowed_users", "");
            buf.set_str(0, "allowed_groups", "");
            int64_t actual_uid = 0;
            if (user_sid) {
                std::string upayload = client_->get_node_payload_async(local_cluster_id_, user_sid).get();
                if (!upayload.empty()) {
                    lite3cpp::Buffer ubuf(upayload);
                    actual_uid = ubuf.get_i64(0, "id");
                }
            }
            buf.set_i64(0, "uid", static_cast<int64_t>(actual_uid != 0 ? actual_uid : (user_sid & 0xFFFFFFFF)));
            buf.set_i64(0, "oid", static_cast<int64_t>(target_int_id != 0 ? target_int_id : (target_sid & 0xFFFFFFFF)));

            client_->put_node_async(local_cluster_id_, sid, buf.move_to_string()).get();

            add_index(EntityType::Ticket, "s", ticket_string, sid);
            add_index(EntityType::Ticket, "id", std::to_string(ticket_id), sid);

            if (target_sid) {
                add_edge(sid, "FOR_OBJECT", 1.0, target_sid);
                add_edge(target_sid, "FOR_OBJECT", 1.0, sid);
            }
            if (user_sid) {
                add_edge(sid, "OWNED_BY", 1.0, user_sid);
                add_edge(user_sid, "OWNED_BY", 1.0, sid);
            }
            snowflake_id_t zid = get_zone_id();
            add_edge(zid, "HAS_TICKET", 1.0, sid);

            return SUCCESS();
        }

        irods::error delete_ticket(std::string_view ticket_string, std::string_view calling_user, bool is_admin) {
            snowflake_id_t sid = resolve_id_from_index(EntityType::Ticket, "s", ticket_string);
            if (!sid) {
                try {
                    sid = resolve_id_from_index(EntityType::Ticket, "id", ticket_string);
                } catch (...) {}
            }
            if (!sid) return CODE(CAT_SUCCESS_BUT_WITH_NO_INFO);

            std::string payload = client_->get_node_payload_async(local_cluster_id_, sid).get();
            if (payload.empty()) return CODE(CAT_SUCCESS_BUT_WITH_NO_INFO);

            lite3cpp::Buffer buf(payload);
            std::string owner = safe_get_str(buf, 0, "own");
            std::string clean_user(calling_user);
            auto hpos = clean_user.find('#');
            if (hpos != std::string::npos) clean_user = clean_user.substr(0, hpos);
            if (!is_admin && !owner.empty() && !calling_user.empty() && owner != calling_user && owner != clean_user) {
                return ERROR(CAT_TICKET_INVALID, "Ticket not owned by user");
            }

            del_index(EntityType::Ticket, "s", ticket_string);
            std::string tid_str = safe_get_str(buf, 0, "id");
            if (!tid_str.empty()) del_index(EntityType::Ticket, "id", tid_str);

            auto targets = client_->get_neighbors_async(local_cluster_id_, sid, "FOR_OBJECT", 0.0).get();
            for (auto tid : targets) {
                del_edge(sid, "FOR_OBJECT", 1.0, tid);
                del_edge(tid, "FOR_OBJECT", 1.0, sid);
            }
            auto owners = client_->get_neighbors_async(local_cluster_id_, sid, "OWNED_BY", 0.0).get();
            for (auto oid : owners) {
                del_edge(sid, "OWNED_BY", 1.0, oid);
                del_edge(oid, "OWNED_BY", 1.0, sid);
            }

            snowflake_id_t zid = get_zone_id();
            del_edge(zid, "HAS_TICKET", 1.0, sid);
            client_->del_node_async(local_cluster_id_, sid).get();
            return SUCCESS();
        }

        irods::error modify_ticket(std::string_view ticket_string, std::string_view op, std::string_view arg1, std::string_view arg2, std::string_view calling_user, bool is_admin) {
            snowflake_id_t sid = resolve_id_from_index(EntityType::Ticket, "s", ticket_string);
            if (!sid) {
                try {
                    sid = resolve_id_from_index(EntityType::Ticket, "id", ticket_string);
                } catch (...) {}
            }
            if (!sid) return ERROR(CAT_TICKET_INVALID, "Ticket invalid");

            std::string payload = client_->get_node_payload_async(local_cluster_id_, sid).get();
            if (payload.empty()) return ERROR(CAT_TICKET_INVALID, "Empty ticket payload");

            lite3cpp::Buffer buf(payload);

            std::string owner = safe_get_str(buf, 0, "own");
            std::string clean_user(calling_user);
            auto hpos = clean_user.find('#');
            if (hpos != std::string::npos) clean_user = clean_user.substr(0, hpos);
            if (!is_admin && !owner.empty() && !calling_user.empty() && owner != calling_user && owner != clean_user) {
                return ERROR(CAT_TICKET_INVALID, "Ticket not owned by user");
            }

            char myTime[32]{};
            getNowStr(myTime);
            buf.set_str(0, "mt", myTime);

            if (op == "uses") {
                try { buf.set_i64(0, "ul", std::stoll(std::string(arg1))); } catch (...) { buf.set_i64(0, "ul", 0); }
            } else if (op == "uses-count") {
                try { buf.set_i64(0, "uc", std::stoll(std::string(arg1))); } catch (...) { buf.set_i64(0, "uc", 0); }
            } else if (op == "write-file" || op == "writefile") {
                try { buf.set_i64(0, "wfl", std::stoll(std::string(arg1))); } catch (...) { buf.set_i64(0, "wfl", 0); }
            } else if (op == "write-file-count") {
                try { buf.set_i64(0, "wfc", std::stoll(std::string(arg1))); } catch (...) { buf.set_i64(0, "wfc", 0); }
            } else if (op == "write-bytes" || op == "writebytes") {
                try { buf.set_i64(0, "wbl", std::stoll(std::string(arg1))); } catch (...) { buf.set_i64(0, "wbl", 0); }
            } else if (op == "write-byte-count") {
                try { buf.set_i64(0, "wbc", std::stoll(std::string(arg1))); } catch (...) { buf.set_i64(0, "wbc", 0); }
            } else if (op == "expire" || op == "expiry") {
                std::string exp = parse_expire_string(arg1);
                buf.set_str(0, "ex", exp);
            } else if (op == "add") {
                if (arg1 == "host") {
                    char* host_ip = convertHostToIp(arg2.data());
                    std::string raw = safe_get_str(buf, 0, "allowed_hosts");
                    auto list = split_csv(raw);
                    if (host_ip && std::find(list.begin(), list.end(), std::string(host_ip)) == list.end()) {
                        list.push_back(host_ip);
                    }
                    if (std::find(list.begin(), list.end(), std::string(arg2)) == list.end()) {
                        list.push_back(std::string(arg2));
                    }
                    buf.set_str(0, "allowed_hosts", join_csv(list));
                } else if (arg1 == "user") {
                    std::string raw = safe_get_str(buf, 0, "allowed_users");
                    auto list = split_csv(raw);
                    std::string u(arg2);
                    if (std::find(list.begin(), list.end(), u) == list.end()) {
                        list.push_back(u);
                    }
                    buf.set_str(0, "allowed_users", join_csv(list));
                } else if (arg1 == "group") {
                    std::string raw = safe_get_str(buf, 0, "allowed_groups");
                    auto list = split_csv(raw);
                    std::string g(arg2);
                    if (std::find(list.begin(), list.end(), g) == list.end()) {
                        list.push_back(g);
                    }
                    buf.set_str(0, "allowed_groups", join_csv(list));
                }
            } else if (op == "remove") {
                if (arg1 == "host") {
                    char* host_ip = convertHostToIp(arg2.data());
                    std::string target = (host_ip ? host_ip : std::string(arg2));
                    std::string raw = safe_get_str(buf, 0, "allowed_hosts");
                    auto list = split_csv(raw);
                    list.erase(std::remove(list.begin(), list.end(), target), list.end());
                    list.erase(std::remove(list.begin(), list.end(), std::string(arg2)), list.end());
                    buf.set_str(0, "allowed_hosts", join_csv(list));
                } else if (arg1 == "user") {
                    std::string raw = safe_get_str(buf, 0, "allowed_users");
                    auto list = split_csv(raw);
                    list.erase(std::remove(list.begin(), list.end(), std::string(arg2)), list.end());
                    buf.set_str(0, "allowed_users", join_csv(list));
                } else if (arg1 == "group") {
                    std::string raw = safe_get_str(buf, 0, "allowed_groups");
                    auto list = split_csv(raw);
                    list.erase(std::remove(list.begin(), list.end(), std::string(arg2)), list.end());
                    buf.set_str(0, "allowed_groups", join_csv(list));
                }
            }

            client_->put_node_async(local_cluster_id_, sid, buf.move_to_string()).get();
            return SUCCESS();
        }

        irods::error get_ticket_restrictions(std::string_view ticket_id_or_str, std::string_view restriction_type, std::vector<std::pair<std::string, std::string>>& out_restrictions) {
            out_restrictions.clear();
            std::string field_name = "allowed_" + std::string(restriction_type) + "s";

            auto process_ticket_node = [&](snowflake_id_t sid) {
                std::string payload = client_->get_node_payload_async(local_cluster_id_, sid).get();
                if (payload.empty()) return;
                lite3cpp::Buffer buf(payload);
                std::string tid = std::to_string(buf.get_i64(0, "id"));
                std::string raw = safe_get_str(buf, 0, field_name);
                auto items = split_csv(raw);
                for (const auto& item : items) {
                    out_restrictions.emplace_back(tid, item);
                }
            };

            if (!ticket_id_or_str.empty()) {
                snowflake_id_t sid = resolve_id_from_index(EntityType::Ticket, "id", ticket_id_or_str);
                if (!sid) sid = resolve_id_from_index(EntityType::Ticket, "s", ticket_id_or_str);
                if (sid) {
                    process_ticket_node(sid);
                }
            } else {
                snowflake_id_t zid = get_zone_id();
                auto t_nodes = client_->get_neighbors_async(local_cluster_id_, zid, "HAS_TICKET", 0.0).get();
                for (auto sid : t_nodes) {
                    process_ticket_node(sid);
                }
            }
            return SUCCESS();
        }

        irods::error validate_ticket(std::string_view ticket_str, std::string_view client_user, std::string_view client_host, std::string* out_target_path = nullptr, std::string* out_target_type = nullptr) {
            snowflake_id_t sid = resolve_id_from_index(EntityType::Ticket, "s", ticket_str);
            if (!sid) {
                try {
                    sid = resolve_id_from_index(EntityType::Ticket, "id", ticket_str);
                } catch (...) {}
            }
            if (!sid) return ERROR(CAT_TICKET_INVALID, "Ticket not found");

            std::string payload = client_->get_node_payload_async(local_cluster_id_, sid).get();
            if (payload.empty()) return ERROR(CAT_TICKET_INVALID, "Empty ticket payload");
            lite3cpp::Buffer buf(payload);

            // Expiration
            std::string ex_str = safe_get_str(buf, 0, "ex");
            if (!ex_str.empty() && ex_str != "0" && ex_str != "00000000000") {
                try {
                    int64_t expiry = std::stoll(ex_str);
                    if (expiry > 0 && expiry <= (int64_t)time(nullptr)) {
                        return ERROR(CAT_TICKET_EXPIRED, "Ticket has expired");
                    }
                } catch (...) {}
            }

            // Host restriction
            std::string allowed_hosts = safe_get_str(buf, 0, "allowed_hosts");
            if (!allowed_hosts.empty()) {
                auto hosts = split_csv(allowed_hosts);
                if (!hosts.empty()) {
                    std::string resolved_client_host;
                    char* cip = convertHostToIp(client_host.data());
                    resolved_client_host = (cip ? cip : std::string(client_host));

                    bool host_match = false;
                    for (const auto& h : hosts) {
                        if (h == client_host || h == resolved_client_host) {
                            host_match = true;
                            break;
                        }
                    }
                    if (!host_match) {
                        return ERROR(CAT_TICKET_HOST_EXCLUDED, "Client host not permitted by ticket");
                    }
                }
            }

            std::string clean_user(client_user);
            auto hash_pos = clean_user.find('#');
            if (hash_pos != std::string::npos) {
                clean_user = clean_user.substr(0, hash_pos);
            }

            // User restriction
            std::string allowed_users = safe_get_str(buf, 0, "allowed_users");
            if (!allowed_users.empty()) {
                auto users = split_csv(allowed_users);
                if (!users.empty()) {
                    bool user_match = false;
                    for (const auto& u : users) {
                        if (u == client_user || u == clean_user ||
                            (!clean_user.empty() && (u.starts_with(clean_user + "#") || clean_user.starts_with(u + "#")))) {
                            user_match = true;
                            break;
                        }
                    }
                    if (!user_match) {
                        return ERROR(CAT_TICKET_USER_EXCLUDED, "Client user not permitted by ticket");
                    }
                }
            }

            // Group restriction
            std::string allowed_groups = safe_get_str(buf, 0, "allowed_groups");
            if (!allowed_groups.empty()) {
                auto groups = split_csv(allowed_groups);
                if (!groups.empty()) {
                    snowflake_id_t usid = resolve_user(client_user);
                    bool group_match = false;
                    if (usid) {
                        auto mem_edges = client_->get_neighbors_async(local_cluster_id_, usid, "MEMBER_OF", 0.0).get();
                        for (auto gid : mem_edges) {
                            std::string gpayload = client_->get_node_payload_async(local_cluster_id_, gid).get();
                            if (!gpayload.empty()) {
                                lite3cpp::Buffer gbuf(gpayload);
                                std::string gname = safe_get_str(gbuf, 0, "n");
                                for (const auto& g : groups) {
                                    if (g == gname || g.starts_with(gname + "#") || gname.starts_with(g + "#")) {
                                        group_match = true;
                                        break;
                                    }
                                }
                            }
                            if (group_match) break;
                        }
                    }
                    if (!group_match) {
                        return ERROR(CAT_TICKET_GROUP_EXCLUDED, "User is not in allowed group");
                    }
                }
            }
            std::string ot = safe_get_str(buf, 0, "ot");
            if (out_target_type) *out_target_type = ot;
            if (out_target_path) {
                std::string tp = safe_get_str(buf, 0, "target_path");
                if (!tp.empty()) {
                    *out_target_path = tp;
                } else {
                    uint64_t oid = buf.get_i64(0, "oid");
                    snowflake_id_t tsid = make_id((ot == "data" ? EntityType::DataObject : EntityType::Collection), oid);
                    std::string tpayload = client_->get_node_payload_async(local_cluster_id_, tsid).get();
                    if (!tpayload.empty()) {
                        lite3cpp::Buffer tbuf(tpayload);
                        if (ot == "data") {
                            *out_target_path = safe_get_str(tbuf, 0, "p");
                        } else {
                            *out_target_path = safe_get_str(tbuf, 0, "n");
                        }
                    }
                }
            }

            return SUCCESS();
        }

        irods::error check_ticket_access(std::string_view ticket_str, snowflake_id_t obj_sid, std::string_view access_type, std::string_view client_user, std::string_view client_host) {
            std::string target_path;
            std::string target_type;
            auto ret = validate_ticket(ticket_str, client_user, client_host, &target_path, &target_type);
            if (!ret.ok()) return ret;

            snowflake_id_t sid = resolve_id_from_index(EntityType::Ticket, "s", ticket_str);
            if (!sid) sid = resolve_id_from_index(EntityType::Ticket, "id", ticket_str);
            if (!sid) return ERROR(CAT_TICKET_INVALID, "Ticket not found");

            std::string payload = client_->get_node_payload_async(local_cluster_id_, sid).get();
            lite3cpp::Buffer buf(payload);

            if (target_path.empty()) {
                target_path = safe_get_str(buf, 0, "target_path");
            }

            // Target object check
            std::string obj_payload = client_->get_node_payload_async(local_cluster_id_, obj_sid).get();
            if (obj_payload.empty()) return ERROR(CAT_TICKET_INVALID, "Object not found");
            lite3cpp::Buffer obj_buf(obj_payload);

            bool obj_match = false;
            std::string obj_p = safe_get_str(obj_buf, 0, "p");
            if (obj_p.empty()) obj_p = safe_get_str(obj_buf, 0, "n");
            uint64_t actual_id = obj_buf.get_i64(0, "id");
            uint64_t int_data_id = (actual_id != 0 ? actual_id : (obj_sid & 0xFFFFFFFF));

            if (target_type == "data") {
                if (!obj_p.empty() && !target_path.empty() && obj_p == target_path) obj_match = true;
                if (!obj_match) {
                    uint64_t oid = buf.get_i64(0, "oid");
                    if (oid != 0 && (oid == int_data_id || oid == (obj_sid & 0xFFFFFFFF))) obj_match = true;
                }
            } else { // target_type == "collection"
                if (!obj_p.empty() && !target_path.empty() && (obj_p == target_path || obj_p.starts_with(target_path == "/" ? "/" : target_path + "/"))) {
                    obj_match = true;
                }
                if (!obj_match) {
                    uint64_t oid = buf.get_i64(0, "oid");
                    if (oid != 0 && (oid == int_data_id || oid == (obj_sid & 0xFFFFFFFF))) obj_match = true;
                }
            }
            if (!obj_match) {
                return ERROR(CAT_TICKET_INVALID, "Ticket does not apply to this object");
            }

            if (target_type == "data") {
                uint64_t current_oid = int_data_id;
                uint64_t saved_oid = buf.get_i64(0, "oid");
                if (current_oid != 0 && current_oid != saved_oid) {
                    buf.set_i64(0, "oid", static_cast<int64_t>(current_oid));
                    if (saved_oid != 0) {
                        snowflake_id_t old_tsid = make_id(EntityType::DataObject, saved_oid);
                        del_edge(sid, "FOR_OBJECT", 1.0, old_tsid);
                        del_edge(old_tsid, "FOR_OBJECT", 1.0, sid);
                    }
                    add_edge(sid, "FOR_OBJECT", 1.0, obj_sid);
                    add_edge(obj_sid, "FOR_OBJECT", 1.0, sid);
                }
            }

            bool is_modify = (access_type == "write" || access_type == "modify");
            std::string t_type = safe_get_str(buf, 0, "t");
            if (is_modify && t_type != "write") {
                return ERROR(CAT_NO_ACCESS_PERMISSION, "Ticket is read-only");
            }

            if (prev_ticket_ != ticket_str) {
                prev_ticket_ = std::string(ticket_str);
                prev_data_id_write_ = 0;
                prev_data_id_uses_ = 0;
            }

            int64_t ul = buf.get_i64(0, "ul");
            int64_t uc = buf.get_i64(0, "uc");
            if (ul > 0 && prev_data_id_uses_ != int_data_id && uc >= ul) {
                return ERROR(CAT_TICKET_USES_EXCEEDED, "Ticket use limit exceeded");
            }

            std::string obj_type = safe_get_str(obj_buf, 0, "t");
            bool is_data_obj = (obj_type != "collection" && (target_type == "data" || obj_type == "data_object" || obj_type == "generic" || safe_get_str(obj_buf, 0, "p").size() > 0));

            if (is_data_obj) {
                if (is_modify) {
                    int64_t wbl = buf.get_i64(0, "wbl");
                    int64_t wbc = buf.get_i64(0, "wbc");
                    if (wbl > 0 && wbc >= wbl) {
                        return ERROR(CAT_TICKET_WRITE_BYTES_EXCEEDED, "Ticket write byte limit exceeded");
                    }

                    int64_t wfl = buf.get_i64(0, "wfl");
                    int64_t wfc = buf.get_i64(0, "wfc");
                    if (wfl > 0 && prev_data_id_write_ != int_data_id && wfc >= wfl) {
                        return ERROR(CAT_TICKET_WRITE_USES_EXCEEDED, "Ticket write file limit exceeded");
                    }
                    if (prev_data_id_write_ != int_data_id) {
                        buf.set_i64(0, "wfc", wfc + 1);
                        prev_data_id_write_ = int_data_id;
                    }
                }

                if (prev_data_id_uses_ != int_data_id) {
                    buf.set_i64(0, "uc", uc + 1);
                    prev_data_id_uses_ = int_data_id;
                }

                client_->put_node_async(local_cluster_id_, sid, buf.move_to_string()).get();
            }
            return SUCCESS();
        }

        irods::error update_ticket_write_bytes(std::string_view ticket_str, snowflake_id_t obj_sid, int64_t bytes) {
            snowflake_id_t sid = resolve_id_from_index(EntityType::Ticket, "s", ticket_str);
            if (!sid) sid = resolve_id_from_index(EntityType::Ticket, "id", ticket_str);
            if (!sid) return ERROR(CAT_TICKET_INVALID, "Ticket not found");
            std::string payload = client_->get_node_payload_async(local_cluster_id_, sid).get();
            if (payload.empty()) return ERROR(CAT_TICKET_INVALID, "Empty ticket payload");
            lite3cpp::Buffer buf(payload);
            int64_t wbc = buf.get_i64(0, "wbc");
            buf.set_i64(0, "wbc", wbc + bytes);
            client_->put_node_async(local_cluster_id_, sid, buf.move_to_string()).get();
            return SUCCESS();
        }

        irods::error increment_ticket_uses(std::string_view ticket_str, uint64_t data_id) {
            snowflake_id_t sid = resolve_id_from_index(EntityType::Ticket, "s", ticket_str);
            if (!sid) sid = resolve_id_from_index(EntityType::Ticket, "id", ticket_str);
            if (!sid) return ERROR(CAT_TICKET_INVALID, "Ticket not found");

            if (prev_ticket_ != ticket_str) {
                prev_ticket_ = std::string(ticket_str);
                prev_data_id_uses_ = 0;
            }
            if (prev_data_id_uses_ == data_id && data_id != 0) {
                return SUCCESS();
            }

            std::string payload = client_->get_node_payload_async(local_cluster_id_, sid).get();
            if (payload.empty()) return ERROR(CAT_TICKET_INVALID, "Empty ticket payload");
            lite3cpp::Buffer buf(payload);

            int64_t ul = buf.get_i64(0, "ul");
            int64_t uc = buf.get_i64(0, "uc");
            if (ul > 0 && uc >= ul) {
                return ERROR(CAT_TICKET_USES_EXCEEDED, "Ticket use limit exceeded");
            }
            buf.set_i64(0, "uc", uc + 1);
            prev_data_id_uses_ = data_id;

            client_->put_node_async(local_cluster_id_, sid, buf.move_to_string()).get();
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
            snowflake_id_t uid = resolve_user(user_name);
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
            snowflake_id_t uid = resolve_user(user_name);
            if (!uid) return SUCCESS();

            std::string q_uuid = "quota:" + std::string(user_name) + ":" + std::string(resc_name);
            snowflake_id_t qid = SnowflakeID::create(local_cluster_id_, q_uuid);

            std::string payload = client_->get_node_payload_async(local_cluster_id_, qid).get();
            if (!payload.empty()) {
                try {
                    lite3cpp::Buffer buf(payload);
                    limit = buf.get_i64(0, "limit");
                } catch (...) {}
            }
            return SUCCESS(); 
        }
        irods::error calculate_usage(std::string_view user_name, std::string_view resc_name, int64_t& usage) {
            usage = 0;
            snowflake_id_t uid = resolve_user(user_name);
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
                            lite3cpp::Buffer buf(payload);
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
            buf.set_str(0, "t", "rule");
            buf.set_str(0, "id", std::to_string(re.id));
            buf.set_str(0, "n", re.name);
            buf.set_str(0, "rei", re.rei_file_path);
            buf.set_str(0, "u", re.user_name);
            buf.set_str(0, "addr", re.address);
            buf.set_str(0, "e", re.exec_time);
            buf.set_str(0, "freq", re.frequency);
            buf.set_str(0, "p", re.priority.empty() ? "5" : re.priority);
            buf.set_str(0, "last", re.last_exec_time);
            buf.set_str(0, "status", re.status);
            buf.set_str(0, "est", re.estimate);
            buf.set_str(0, "notif", re.notification_addr);
            buf.set_str(0, "ctx", re.context);
            buf.set_str(0, "lh", re.lock_host);
            buf.set_str(0, "lp", re.lock_host_pid);
            buf.set_str(0, "lt", re.lock_time);
            client_->put_node_async(local_cluster_id_, rid, buf.move_to_string()).get();
            snowflake_id_t zid = get_zone_id();
            add_edge(zid, "HAS_RULE", 1.0, rid);
            out_id = re.id; 
            return SUCCESS();
        }

        irods::error get_rule_execution(uint64_t id, rule_exec& out_re) {
            snowflake_id_t rid = make_id(EntityType::Rule, id);
            std::string payload = client_->get_node_payload_async(local_cluster_id_, rid).get();
            if (payload.empty()) {
                return ERROR(CAT_NO_ROWS_FOUND, "Rule execution not found");
            }
            lite3cpp::Buffer buf(payload);
            out_re.id = id;
            out_re.name = safe_get_str(buf, 0, "n");
            out_re.rei_file_path = safe_get_str(buf, 0, "rei");
            out_re.user_name = safe_get_str(buf, 0, "u");
            out_re.address = safe_get_str(buf, 0, "addr");
            out_re.exec_time = safe_get_str(buf, 0, "e");
            out_re.frequency = safe_get_str(buf, 0, "freq");
            out_re.priority = safe_get_str(buf, 0, "p");
            if (out_re.priority.empty()) out_re.priority = "5";
            out_re.last_exec_time = safe_get_str(buf, 0, "last");
            out_re.status = safe_get_str(buf, 0, "status");
            out_re.estimate = safe_get_str(buf, 0, "est");
            out_re.notification_addr = safe_get_str(buf, 0, "notif");
            out_re.context = safe_get_str(buf, 0, "ctx");
            out_re.lock_host = safe_get_str(buf, 0, "lh");
            out_re.lock_host_pid = safe_get_str(buf, 0, "lp");
            out_re.lock_time = safe_get_str(buf, 0, "lt");
            return SUCCESS();
        }

        irods::error lock_rule_execution(uint64_t id, std::string_view lock_host, int lock_host_pid) {
            snowflake_id_t rid = make_id(EntityType::Rule, id);
            std::string payload = client_->get_node_payload_async(local_cluster_id_, rid).get();
            if (payload.empty()) {
                return ERROR(CAT_NO_ROWS_UPDATED, "Rule execution not found");
            }
            lite3cpp::Buffer buf(payload);
            std::string current_lh = safe_get_str(buf, 0, "lh");
            if (!current_lh.empty()) {
                return ERROR(CAT_NO_ROWS_UPDATED, "Rule already locked");
            }
            buf.set_str(0, "lh", std::string(lock_host));
            buf.set_str(0, "lp", std::to_string(lock_host_pid));
            buf.set_str(0, "lt", std::to_string(std::time(nullptr)));
            client_->put_node_async(local_cluster_id_, rid, buf.move_to_string()).get();
            return SUCCESS();
        }

        irods::error unlock_rule_execution(uint64_t id) {
            snowflake_id_t rid = make_id(EntityType::Rule, id);
            std::string payload = client_->get_node_payload_async(local_cluster_id_, rid).get();
            if (payload.empty()) {
                return SUCCESS();
            }
            lite3cpp::Buffer buf(payload);
            buf.set_str(0, "lh", "");
            buf.set_str(0, "lp", "");
            buf.set_str(0, "lt", "");
            client_->put_node_async(local_cluster_id_, rid, buf.move_to_string()).get();
            return SUCCESS();
        }

        irods::error delete_rule_execution(uint64_t id) {
            snowflake_id_t rid = make_id(EntityType::Rule, id);
            client_->del_node_async(local_cluster_id_, rid).get();
            snowflake_id_t zid = get_zone_id();
            del_edge(zid, "HAS_RULE", 1.0, rid);
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
        irods::error has_specific_query(std::string_view alias_or_sql, bool& out_has) {
            out_has = false;
            std::string q(alias_or_sql);
            snowflake_id_t sid = SnowflakeID::create(local_cluster_id_, "sq:" + q);
            std::string payload = client_->get_node_payload_async(local_cluster_id_, sid).get();
            if (!payload.empty()) {
                out_has = true;
                return SUCCESS();
            }
            if (q == "ShowCollAcls" || q.find("ShowCollAcls") != std::string::npos ||
                (q.find("R_COLL_MAIN") != std::string::npos && q.find("R_OBJT_ACCESS") != std::string::npos)) {
                sid = SnowflakeID::create(local_cluster_id_, "sq:ShowCollAcls");
                payload = client_->get_node_payload_async(local_cluster_id_, sid).get();
                if (!payload.empty()) {
                    out_has = true;
                    return SUCCESS();
                }
            }
            return SUCCESS();
        }
        irods::error get_collection_acls(std::string_view coll_name, std::vector<AclEntry>& out_acls) {
            out_acls.clear();
            std::string clean_coll(coll_name);
            while (clean_coll.size() > 1 && clean_coll.back() == '/') {
                clean_coll.pop_back();
            }

            snowflake_id_t cid = resolve_id_from_index(EntityType::Collection, "n", clean_coll);
            if (!cid) {
                return ERROR(CAT_NO_ROWS_FOUND, "Collection not found");
            }

            auto aids = client_->get_in_neighbors_async(local_cluster_id_, cid, "FOR_OBJECT").get();
            std::set<std::tuple<std::string, std::string, std::string, std::string>> seen;

            for (snowflake_id_t aid : aids) {
                std::string payload = client_->get_node_payload_async(local_cluster_id_, aid).get();
                if (payload.empty()) continue;

                lite3cpp::Buffer buf(payload);
                std::string level = safe_get_str(buf, 0, "l");
                std::string uname = safe_get_str(buf, 0, "u");
                std::string uzone = safe_get_str(buf, 0, "z");
                int64_t uid_val = 0;
                try { uid_val = buf.get_i64(0, "uid"); } catch (...) {}

                std::string utype = "rodsuser";
                if (uid_val > 0) {
                    snowflake_id_t uid = static_cast<snowflake_id_t>(uid_val);
                    std::string u_payload = client_->get_node_payload_async(local_cluster_id_, uid).get();
                    if (!u_payload.empty()) {
                        lite3cpp::Buffer ubuf(u_payload);
                        std::string t = safe_get_str(ubuf, 0, "t");
                        if (!t.empty()) utype = t;
                        if (uname.empty()) uname = safe_get_str(ubuf, 0, "n");
                        if (uzone.empty()) uzone = safe_get_str(ubuf, 0, "z");
                    }
                }

                if (uname.empty()) {
                    auto uids = client_->get_in_neighbors_async(local_cluster_id_, aid, "HAS_ACCESS").get();
                    snowflake_id_t group_uid = 0;
                    std::string group_n, group_z, group_t;
                    for (snowflake_id_t uid : uids) {
                        std::string u_payload = client_->get_node_payload_async(local_cluster_id_, uid).get();
                        if (u_payload.empty()) continue;
                        lite3cpp::Buffer ubuf(u_payload);
                        std::string t = safe_get_str(ubuf, 0, "t");
                        if (t == "rodsgroup") {
                            group_uid = uid;
                            group_n = safe_get_str(ubuf, 0, "n");
                            group_z = safe_get_str(ubuf, 0, "z");
                            group_t = t;
                            break;
                        }
                    }
                    if (group_uid) {
                        if (group_z.empty()) group_z = local_zone_name_;
                        if (!seen.count({group_n, group_z, level, group_t})) {
                            seen.insert({group_n, group_z, level, group_t});
                            out_acls.push_back({group_n, group_z, level, group_t});
                        }
                    } else {
                        for (snowflake_id_t uid : uids) {
                            std::string u_payload = client_->get_node_payload_async(local_cluster_id_, uid).get();
                            if (u_payload.empty()) continue;
                            lite3cpp::Buffer ubuf(u_payload);
                            std::string t = safe_get_str(ubuf, 0, "t");
                            std::string n = safe_get_str(ubuf, 0, "n");
                            std::string z = safe_get_str(ubuf, 0, "z");
                            if (z.empty()) z = local_zone_name_;
                            if (!seen.count({n, z, level, t})) {
                                seen.insert({n, z, level, t});
                                out_acls.push_back({n, z, level, t});
                            }
                        }
                    }
                    continue;
                }

                if (uzone.empty()) uzone = local_zone_name_;
                if (!seen.count({uname, uzone, level, utype})) {
                    seen.insert({uname, uzone, level, utype});
                    out_acls.push_back({uname, uzone, level, utype});
                }
            }

            if (out_acls.empty()) {
                return ERROR(CAT_NO_ROWS_FOUND, "No ACLs found");
            }
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
                    lite3cpp::Buffer buf(payload);
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
                            lite3cpp::Buffer buf(payload);
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
            snowflake_id_t zid = get_zone_id();
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
            if (payload.empty()) {
                if (key == "delay_server:leader") {
                    char h[1024];
                    if (gethostname(h, sizeof(h)) == 0) {
                        out_value = h;
                        set_grid_configuration_value(key, out_value);
                        set_grid_configuration_value("delay_server:successor", "");
                        return SUCCESS();
                    }
                } else if (key == "delay_server:successor") {
                    out_value = "";
                    return SUCCESS();
                }
                return ERROR(CAT_NO_ROWS_FOUND, "Grid configuration value not found");
            }
            try {
                lite3cpp::Buffer buf(payload);
                out_value = safe_get_str(buf, 0, "v");
            } catch (...) { return ERROR(-1, "Failed to parse grid config"); }
            return SUCCESS();
        }

        // --- Query ---

        irods::error execute_query(const irods::experimental::genquery2::select& ast, ResultSet& results, const std::vector<uint64_t>& starting_nodes = {}, std::string_view root_type = "", const irods::experimental::genquery2::options* opts = nullptr) {
            try {
                std::string effective_root_type(root_type);
                std::vector<uint64_t> sn = starting_nodes;
                if (!sn.empty() && sn[0] == 0) sn.clear();


                compiler::Gq2ToL3kvgCompiler compiler;
                lite3cpp::Buffer query_buf = compiler.compile(ast, effective_root_type, opts);
                if (effective_root_type.empty()) {
                    effective_root_type = compiler.get_entry_type();
                }

                if (sn.empty() && effective_root_type == "Zone") {
                    snowflake_id_t zid = get_zone_id();
                    sn.push_back(zid);
                } else if (sn.empty() && effective_root_type == "Resource") {
                    snowflake_id_t zid = get_zone_id();
                    auto resc_nodes = client_->get_neighbors_async(local_cluster_id_, zid, "HAS_RESC", 0.0).get();
                    sn = std::move(resc_nodes);
                } else if (sn.empty() && (effective_root_type == "User" || effective_root_type == "Group")) {
                    snowflake_id_t zid = get_zone_id();
                    auto user_nodes = client_->get_neighbors_async(local_cluster_id_, zid, "HAS_USER", 0.0).get();
                    sn = std::move(user_nodes);
                } else if (sn.empty() && effective_root_type == "Rule") {
                    snowflake_id_t zid = get_zone_id();
                    auto rule_nodes = client_->get_neighbors_async(local_cluster_id_, zid, "HAS_RULE", 0.0).get();
                    sn = std::move(rule_nodes);
                    if (sn.empty()) {
                        results.rows.clear();
                        return SUCCESS();
                    }
                }

            #ifdef IRODS_SERVER
            rodsLog(LOG_DEBUG, "L3_CATALOG: Executing Query with root_type [%s] and [%zu] starting nodes (query buffer size: %zu bytes)", effective_root_type.c_str(), sn.size(), query_buf.size());
            #endif

                auto fut = client_->resume_query_async(local_cluster_id_, sn, query_buf);
                results.rows = fut.get();

            #ifdef IRODS_SERVER
            rodsLog(LOG_DEBUG, "L3_CATALOG: Query returned %zu rows", results.rows.size());
            for (size_t r = 0; r < std::min(results.rows.size(), size_t(5)); ++r) {
                for (const auto& [k, v] : results.rows[r].fields) {
                    rodsLog(LOG_DEBUG, "L3_CATALOG: Result Row %zu: [%s]=[%s]", r, k.c_str(), v.c_str());
                }
            }
            #endif

            return SUCCESS();
            } catch (const std::invalid_argument& e) {
            #ifdef IRODS_SERVER
                rodsLog(LOG_ERROR, "L3KVG: execute_query invalid argument: %s", e.what());
            #endif
                return ERROR(SYS_INVALID_INPUT_PARAM, e.what());
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

        irods::error execute_dml(const compiler::DmlPlan& plan, lite3cpp::Buffer& result) {
            try {
                auto set_dml_result = [&](int64_t rows_affected) {
                    result.clear();
                    result.init_object();
                    result.set_i64(0, "rows_affected", rows_affected);
                    result.set_str(0, "status", "SUCCESS");
                };

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

                if (plan.entity_type == "USER_GROUP" || plan.entity_type == "user_group" || plan.entity_type == "R_USER_GROUP") {
                    if (plan.action == compiler::DmlAction::Insert) {
                        std::string user_id, group_user_id;
                        for (const auto& [k, v] : plan.properties) {
                            if (k == "user_id" || k == "u_id") user_id = v;
                            else if (k == "group_user_id" || k == "g_id" || k == "id") group_user_id = v;
                        }
                        if (user_id.empty() && plan.properties.count("user_id")) user_id = plan.properties.at("user_id");
                        if (!user_id.empty() && !group_user_id.empty()) {
                            try {
                                uint64_t u_num = std::stoull(user_id);
                                uint64_t g_num = std::stoull(group_user_id);
                                snowflake_id_t uid = make_id(EntityType::User, u_num);
                                snowflake_id_t gid = make_id(EntityType::User, g_num);
                                add_edge(uid, "MEMBER_OF", 1.0, gid);
                            } catch (...) {}
                        }
                        set_dml_result(1);
                        return SUCCESS();
                    } else if (plan.action == compiler::DmlAction::Remove) {
                        uint64_t uid_val = 0, gid_val = 0;
                        for (const auto& cond : plan.conditions) {
                            if (cond.op == 0) {
                                if (cond.property == "user_id" || cond.property == "u_id") {
                                     try { uid_val = std::stoull(cond.value); } catch (...) {}
                                } else if (cond.property == "group_user_id" || cond.property == "id" || cond.property == "g_id") {
                                     try { gid_val = std::stoull(cond.value); } catch (...) {}
                                }
                            }
                        }
                        if (uid_val != 0 && gid_val != 0) {
                            snowflake_id_t uid = make_id(EntityType::User, uid_val);
                            snowflake_id_t gid = make_id(EntityType::User, gid_val);
                            del_edge(uid, "MEMBER_OF", 1.0, gid);
                        } else if (uid_val != 0) {
                            snowflake_id_t uid = make_id(EntityType::User, uid_val);
                            auto groups = client_->get_neighbors_async(local_cluster_id_, uid, "MEMBER_OF", 0.0).get();
                            for (auto g : groups) del_edge(uid, "MEMBER_OF", 1.0, g);
                        } else if (gid_val != 0) {
                            snowflake_id_t gid = make_id(EntityType::User, gid_val);
                            auto members = client_->get_in_neighbors_async(local_cluster_id_, gid, "MEMBER_OF").get();
                            for (auto u : members) del_edge(u, "MEMBER_OF", 1.0, gid);
                        }
                        set_dml_result(1);
                        return SUCCESS();
                    }
                }

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
                    if (n_it == plan.properties.end()) n_it = plan.properties.find("data_name");
                    if (n_it == plan.properties.end()) n_it = plan.properties.find("coll_name");
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

                    if (parent_coll.empty()) {
                        auto cid_it = plan.properties.find("coll_id");
                        if (cid_it == plan.properties.end()) cid_it = plan.properties.find("DATA_COLL_ID");
                        if (cid_it != plan.properties.end() && !cid_it->second.empty()) {
                            try {
                                snowflake_id_t psid = make_id(EntityType::Collection, std::stoull(cid_it->second));
                                std::string payload = client_->get_node_payload_async(local_cluster_id_, psid).get();
                                if (!payload.empty()) {
                                    lite3cpp::Buffer pbuf(payload);
                                    parent_coll = safe_get_str(pbuf, 0, "n");
                                }
                            } catch (...) {}
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
                        if (k == "parent_coll" || k == "parent_collection" || k == "pn" || k == "coll_id") continue;
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

                    if (et == EntityType::DataObject) {
                        buf.set_str(0, "entity_type", "data_object");
                    } else if (et == EntityType::Collection) {
                        buf.set_str(0, "entity_type", "collection");
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

                    if (irods_id != 0) {
                        add_index(et, "id", std::to_string(irods_id), sid);
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
                        std::string owner_zone;
                        if (et == EntityType::DataObject || et == EntityType::Collection) {
                            auto z_it = plan.properties.find("z");
                            if (z_it == plan.properties.end()) z_it = plan.properties.find("data_owner_zone");
                            if (z_it == plan.properties.end()) z_it = plan.properties.find("coll_owner_zone");
                            if (z_it == plan.properties.end()) z_it = plan.properties.find("zone_name");
                            if (z_it != plan.properties.end()) owner_zone = z_it->second;
                        }
                        snowflake_id_t user_sid = resolve_user(owner_name, owner_zone);
                        if (user_sid) {
                            add_edge(user_sid, "OWNS", 1.0, sid);
                        }
                        if (et == EntityType::DataObject || et == EntityType::Collection) {
                            set_access(owner_name, owner_zone, (full_path.empty() ? name : full_path), "own", false);
                        }
                    }

                    set_dml_result(1);
                    return SUCCESS();
                } else if (plan.action == compiler::DmlAction::Update) {
                    snowflake_id_t sid = resolve_target_sid(et, plan.conditions);

                    if (sid == 0) {
                        set_dml_result(0);
                        return SUCCESS();
                    }

                    std::string payload = client_->get_node_payload_async(local_cluster_id_, sid).get();
                    if (payload.empty()) {
                        set_dml_result(0);
                        return SUCCESS();
                    }

                    lite3cpp::Buffer buf(payload);

                    // Secondary filter verification against buf
                    for (const auto& cond : plan.conditions) {
                        std::string actual = get_prop_val(buf, cond.property);
                        if (!compare_vals(actual, cond.op, cond.value)) {
                            set_dml_result(0);
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

                    set_dml_result(1);
                    return SUCCESS();
                } else if (plan.action == compiler::DmlAction::Remove) {
                    snowflake_id_t sid = resolve_target_sid(et, plan.conditions);

                    if (sid == 0) {
                        set_dml_result(0);
                        return SUCCESS();
                    }

                    std::string payload = client_->get_node_payload_async(local_cluster_id_, sid).get();
                    if (payload.empty()) {
                        set_dml_result(0);
                        return SUCCESS();
                    }

                    lite3cpp::Buffer buf(payload);

                    // Secondary filter verification against buf
                    for (const auto& cond : plan.conditions) {
                        std::string actual = get_prop_val(buf, cond.property);
                        if (!compare_vals(actual, cond.op, cond.value)) {
                            set_dml_result(0);
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
                    if (!name.empty()) del_index(et, "n", name, sid);
                    std::string path = safe_get_str(buf, 0, "p");
                    if (!path.empty()) del_index(et, "path", path);
                    std::string id_str = safe_get_str(buf, 0, "id");
                    if (!id_str.empty()) del_index(et, "id", id_str);

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

                    set_dml_result(1);
                    return SUCCESS();
                }

                return ERROR(-1, "Unsupported DML action");
            } catch (const std::exception& e) {
                return ERROR(-1, e.what());
            } catch (...) {
                return ERROR(-1, "Unknown exception in execute_dml");
            }
        }

        void reset_ticket_session_state() {
            prev_data_id_write_ = 0;
            prev_data_id_uses_ = 0;
            prev_ticket_.clear();
        }

    private:
        std::unique_ptr<l3kvg::RemoteL3KVClient> client_;
        uint16_t local_cluster_id_ = 0;
        std::string local_zone_name_;
        uint64_t prev_data_id_write_ = 0;
        uint64_t prev_data_id_uses_ = 0;
        std::string prev_ticket_;
        std::mutex objid_mu_;
        uint64_t objid_curr_{0};
        uint64_t objid_limit_{0};
    };

    CatalogFacade::CatalogFacade() : pImpl_(std::make_unique<CatalogImpl>()) {}
    CatalogFacade::~CatalogFacade() = default;
    irods::error CatalogFacade::init(const Config& cfg, std::string_view zone_name, const l3kvg::Settings& settings) { return pImpl_->init(cfg, zone_name, settings); }
    irods::error CatalogFacade::bootstrap_catalog(std::string_view zone_name, std::string_view admin_name) { return pImpl_->bootstrap_catalog(zone_name, admin_name); }
    irods::error CatalogFacade::bootstrap_federation(const std::vector<FederatedZone>& peers) { return pImpl_->bootstrap_federation(peers); }
    irods::error CatalogFacade::register_data_object(const data_object& obj, data_id_t& out_id, const replica* initial_repl) { return pImpl_->register_data_object(obj, out_id, initial_repl); }
    irods::error CatalogFacade::delete_data_object(data_id_t id) { return pImpl_->delete_data_object(id); }
    irods::error CatalogFacade::rename_data_object(data_id_t obj_id, std::string_view new_name) { return pImpl_->rename_data_object(obj_id, new_name); }
    irods::error CatalogFacade::move_data_object(data_id_t obj_id, coll_id_t target_coll_id) { return pImpl_->move_data_object(obj_id, target_coll_id); }
    irods::error CatalogFacade::modify_data_object(data_id_t obj_id, std::string_view prop, std::string_view value) { return pImpl_->modify_data_object(obj_id, prop, value); }
    irods::error CatalogFacade::rename_object(uint64_t obj_id, std::string_view new_name) { return pImpl_->rename_object(obj_id, new_name); }
    irods::error CatalogFacade::move_object(uint64_t obj_id, coll_id_t target_coll_id) { return pImpl_->move_object(obj_id, target_coll_id); }
    irods::error CatalogFacade::register_replica(const replica& repl) { return pImpl_->register_replica(repl); }
    irods::error CatalogFacade::unregister_replica(data_id_t data_id, uint32_t repl_num) { return pImpl_->unregister_replica(data_id, repl_num); }
    irods::error CatalogFacade::update_replica_access_time(data_id_t data_id, uint32_t repl_num, std::string_view time) { return pImpl_->update_replica_access_time(data_id, repl_num, time); }
    irods::error CatalogFacade::modify_replicas_for_data_object(data_id_t obj_id, uint32_t repl_num, std::string_view resc_hier, const std::vector<std::pair<std::string, std::string>>& updates, bool all_repl_status, bool all_replicas) { return pImpl_->modify_replicas_for_data_object(obj_id, repl_num, resc_hier, updates, all_repl_status, all_replicas); }
    uint32_t CatalogFacade::get_next_replica_number(data_id_t data_id) { return pImpl_->get_next_replica_number(data_id); }
    irods::error CatalogFacade::register_collection(const collection& coll, coll_id_t& out_id) { return pImpl_->register_collection(coll, out_id); }
    irods::error CatalogFacade::rename_collection(std::string_view old_name, std::string_view new_name) { return pImpl_->rename_collection(old_name, new_name); }
    irods::error CatalogFacade::delete_collection(coll_id_t coll_id) { return pImpl_->delete_collection(coll_id); }
    irods::error CatalogFacade::is_collection_empty(coll_id_t coll_id, bool& is_empty) { return pImpl_->is_collection_empty(coll_id, is_empty); }
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
    irods::error CatalogFacade::delete_user(std::string_view user_name, std::string_view zone) { return pImpl_->delete_user(user_name, zone); }
    irods::error CatalogFacade::modify_user(std::string_view user_name, std::string_view prop, std::string_view value, std::string_view zone) { return pImpl_->modify_user(user_name, prop, value, zone); }
    snowflake_id_t CatalogFacade::resolve_user(std::string_view user_name, std::string_view zone) { return pImpl_->resolve_user(user_name, zone); }
    irods::error CatalogFacade::check_auth(std::string_view user_name, std::string_view zone, int& user_priv) { return pImpl_->check_auth(user_name, zone, user_priv); }
    irods::error CatalogFacade::get_user_password_and_priv(std::string_view user_name, std::string_view zone, std::string& out_pw, int& out_priv) { return pImpl_->get_user_password_and_priv(user_name, zone, out_pw, out_priv); }
    irods::error CatalogFacade::check_auth_credentials(std::string_view username, std::string_view zone, std::string_view password, bool& correct) { return pImpl_->check_auth_credentials(username, zone, password, correct); }
    irods::error CatalogFacade::add_user_to_group(std::string_view user_name, std::string_view zone, std::string_view group_name) { return pImpl_->add_user_to_group(user_name, zone, group_name); }
    irods::error CatalogFacade::remove_user_from_group(std::string_view user_name, std::string_view zone, std::string_view group_name) { return pImpl_->remove_user_from_group(user_name, zone, group_name); }
    irods::error CatalogFacade::set_access(std::string_view user_name, std::string_view zone, std::string_view target_path, std::string_view level, bool recursive) { return pImpl_->set_access(user_name, zone, target_path, level, recursive); }
    irods::error CatalogFacade::check_permission(snowflake_id_t user_id, snowflake_id_t target_id, std::string_view level, bool& allowed, bool check_parents) { return pImpl_->check_permission(user_id, target_id, level, allowed, check_parents); }
    irods::error CatalogFacade::check_permission_to_modify_data_object(snowflake_id_t user_id, snowflake_id_t target_id, bool& allowed) { return pImpl_->check_permission_to_modify_data_object(user_id, target_id, allowed); }

    irods::error CatalogFacade::add_avu_metadata(std::string_view type, std::string_view target_id, const avu& metadata) { return pImpl_->add_avu_metadata(type, target_id, metadata); }
    irods::error CatalogFacade::delete_avu_metadata(std::string_view type, std::string_view target_id, const avu& metadata, int option) { return pImpl_->delete_avu_metadata(type, target_id, metadata, option); }
    irods::error CatalogFacade::modify_avu_metadata(std::string_view type, std::string_view target_id, const avu& old_avu, const avu& new_avu) { return pImpl_->modify_avu_metadata(type, target_id, old_avu, new_avu); }
    irods::error CatalogFacade::copy_avu_metadata(std::string_view src_type, std::string_view src_id, std::string_view dst_type, std::string_view dst_id) { return pImpl_->copy_avu_metadata(src_type, src_id, dst_type, dst_id); }
    irods::error CatalogFacade::set_avu_metadata(std::string_view type, std::string_view target_id, const avu& metadata) { return pImpl_->set_avu_metadata(type, target_id, metadata); }
    snowflake_id_t CatalogFacade::resolve_target_entity_sid(std::string_view type, std::string_view target_id_or_name) { return pImpl_->resolve_target_entity_sid(type, target_id_or_name); }
    irods::error CatalogFacade::register_zone(const zone& z) { return pImpl_->register_zone(z); }
    irods::error CatalogFacade::modify_zone(std::string_view name, std::string_view prop, std::string_view value) { return pImpl_->modify_zone(name, prop, value); }
    irods::error CatalogFacade::delete_zone(std::string_view name) { return pImpl_->delete_zone(name); }
    irods::error CatalogFacade::create_ticket(uint64_t ticket_id, std::string_view ticket_string, std::string_view ticket_type, snowflake_id_t target_sid, EntityType target_type, snowflake_id_t user_sid, std::string_view user_name, std::string_view target_path) { return pImpl_->create_ticket(ticket_id, ticket_string, ticket_type, target_sid, target_type, user_sid, user_name, target_path); }
    irods::error CatalogFacade::delete_ticket(std::string_view ticket_string, std::string_view calling_user, bool is_admin) { return pImpl_->delete_ticket(ticket_string, calling_user, is_admin); }
    irods::error CatalogFacade::modify_ticket(std::string_view ticket_string, std::string_view op, std::string_view arg1, std::string_view arg2, std::string_view calling_user, bool is_admin) { return pImpl_->modify_ticket(ticket_string, op, arg1, arg2, calling_user, is_admin); }
    irods::error CatalogFacade::get_ticket_restrictions(std::string_view ticket_id_or_str, std::string_view restriction_type, std::vector<std::pair<std::string, std::string>>& out_restrictions) { return pImpl_->get_ticket_restrictions(ticket_id_or_str, restriction_type, out_restrictions); }
    irods::error CatalogFacade::validate_ticket(std::string_view ticket_str, std::string_view client_user, std::string_view client_host, std::string* out_target_path, std::string* out_target_type) { return pImpl_->validate_ticket(ticket_str, client_user, client_host, out_target_path, out_target_type); }
    irods::error CatalogFacade::check_ticket_access(std::string_view ticket_str, snowflake_id_t obj_sid, std::string_view access_type, std::string_view client_user, std::string_view client_host) { return pImpl_->check_ticket_access(ticket_str, obj_sid, access_type, client_user, client_host); }
    irods::error CatalogFacade::update_ticket_write_bytes(std::string_view ticket_str, snowflake_id_t obj_sid, int64_t bytes) { return pImpl_->update_ticket_write_bytes(ticket_str, obj_sid, bytes); }
    irods::error CatalogFacade::increment_ticket_uses(std::string_view ticket_str, uint64_t data_id) { return pImpl_->increment_ticket_uses(ticket_str, data_id); }
    void CatalogFacade::reset_ticket_session_state() { pImpl_->reset_ticket_session_state(); }
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
    irods::error CatalogFacade::get_rule_execution(uint64_t id, rule_exec& out_re) { return pImpl_->get_rule_execution(id, out_re); }
    irods::error CatalogFacade::lock_rule_execution(uint64_t id, std::string_view lock_host, int lock_host_pid) { return pImpl_->lock_rule_execution(id, lock_host, lock_host_pid); }
    irods::error CatalogFacade::unlock_rule_execution(uint64_t id) { return pImpl_->unlock_rule_execution(id); }
    irods::error CatalogFacade::delete_rule_execution(uint64_t id) { return pImpl_->delete_rule_execution(id); }

    // Specific Query Operations
    irods::error CatalogFacade::register_specific_query(std::string_view alias, std::string_view sql) { return pImpl_->register_specific_query(alias, sql); }
    irods::error CatalogFacade::delete_specific_query(std::string_view alias) { return pImpl_->delete_specific_query(alias); }
    irods::error CatalogFacade::has_specific_query(std::string_view alias_or_sql, bool& out_has) { return pImpl_->has_specific_query(alias_or_sql, out_has); }
    irods::error CatalogFacade::get_collection_acls(std::string_view coll_name, std::vector<AclEntry>& out_acls) { return pImpl_->get_collection_acls(coll_name, out_acls); }

    irods::error CatalogFacade::resolve_path(std::string_view path, snowflake_id_t& out_id, EntityType& out_type) { return pImpl_->resolve_path(path, out_id, out_type); }
    irods::error CatalogFacade::get_collection_subtree_ids(snowflake_id_t coll_sid, std::vector<snowflake_id_t>& out_ids) { return pImpl_->get_collection_subtree_ids(coll_sid, out_ids); }
    irods::error CatalogFacade::get_child_collection_ids(snowflake_id_t parent_sid, std::string_view parent_path, std::vector<snowflake_id_t>& out_ids) { return pImpl_->get_child_collection_ids(parent_sid, parent_path, out_ids); }
    irods::error CatalogFacade::execute_query(const irods::experimental::genquery2::select& ast, ResultSet& results, const std::vector<uint64_t>& starting_nodes, std::string_view root_type, const irods::experimental::genquery2::options* opts) { return pImpl_->execute_query(ast, results, starting_nodes, root_type, opts); }
    irods::error CatalogFacade::execute_dml(const compiler::DmlPlan& plan, lite3cpp::Buffer& result) { return pImpl_->execute_dml(plan, result); }
    irods::error CatalogFacade::apply_atomic_operations(const std::vector<irods::experimental::dml::operation_type>& ops) { return pImpl_->apply_atomic_operations(ops); }
    irods::error CatalogFacade::get_next_sequence_value(std::string_view seq_name, uint64_t& out_val) { return pImpl_->get_next_sequence_value(seq_name, out_val); }
    snowflake_id_t CatalogFacade::make_id(EntityType type, uint64_t irods_id) { return pImpl_->make_id(type, irods_id); }
    snowflake_id_t CatalogFacade::resolve_id_from_index(EntityType type, std::string_view attr, std::string_view value) { return pImpl_->resolve_id_from_index(type, attr, value); }
    snowflake_id_t CatalogFacade::get_zone_id(std::string_view zname) const { return pImpl_->get_zone_id(zname); }
    const std::string& CatalogFacade::get_local_zone_name() const { return pImpl_->get_local_zone_name(); }

    l3kvg::RemoteL3KVClient* CatalogFacade::get_client() const { return pImpl_->get_client(); }
    uint16_t CatalogFacade::get_cluster_id() const { return pImpl_->get_cluster_id(); }

} // namespace irods::catalog
