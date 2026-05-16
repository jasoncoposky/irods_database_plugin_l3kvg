#include "irods/catalog/catalog_facade.hpp"
#include "L3KVG/Engine.hpp"
#include "L3KVG/Node.hpp"
#include "engine/store.hpp"
#include "irods/catalog/l3kvg_mapper.hpp"
#include "irods/catalog/catalog_schemas.hpp"
#include "irods/catalog/gq2_compiler.hpp"
#include <iostream>
#include <cstdio>

#ifdef IRODS_SERVER
#include "irods/rodsLog.h"
#define CAT_LOG(level, ...) rodsLog(level, __VA_ARGS__)
#else
#define CAT_LOG(level, ...) std::printf(__VA_ARGS__); std::printf("\n")
#endif

namespace irods::catalog {

    class CatalogImpl {
    public:
        CatalogImpl() = default;

        irods::error init(const Config& cfg) {
            try {
                if (engine_) {
                    return ERROR(-1, "Catalog already initialized");
                }
                engine_ = std::make_unique<l3kvg::Engine>(cfg.db_path, cfg.node_id);
                return SUCCESS();
            } catch (const std::exception& e) {
                return ERROR(-1, e.what());
            }
        }

        irods::error execute_query(const irods::experimental::genquery2::select& ast, ResultSet& results) {
            if (!engine_) return ERROR(-1, "Catalog not initialized");
            try {
                compiler::Gq2ToL3kvgCompiler compiler(*engine_);
                auto query = compiler.compile(ast);
                results.rows = query.execute();
                
                CAT_LOG(LOG_NOTICE, "L3KVG: execute_query produced %zu rows", results.rows.size());
                return SUCCESS();
            } catch (const std::exception& e) {
                CAT_LOG(LOG_ERROR, "L3KVG: execute_query exception: %s", e.what());
                return ERROR(-1, e.what());
            }
        }

        irods::error bootstrap_catalog(std::string_view zone_name, std::string_view admin_name) {
            if (!engine_) return ERROR(-1, "Catalog not initialized");
            
            // 1. Register Zone
            zone z{std::string(zone_name), "local", ""};
            if (auto ret = register_zone(z); !ret.ok()) return ret;

            // 2. Register Admin User
            user u;
            u.id = 1;
            u.name = std::string(admin_name);
            u.zone = std::string(zone_name);
            u.type = "rodsadmin";
            user_id_t out_uid;
            if (auto ret = register_user(u, out_uid); !ret.ok()) return ret;

            // 3. Register Root Collection /zone
            collection root_coll;
            root_coll.id = 1000;
            root_coll.name = "/" + std::string(zone_name);
            root_coll.parent_id = 0;
            root_coll.owner_name = std::string(admin_name);
            root_coll.owner_zone = std::string(zone_name);
            coll_id_t out_cid;
            if (auto ret = register_collection(root_coll, out_cid); !ret.ok()) return ret;

            // 4. Register /zone/home
            collection home_root;
            home_root.id = 1001;
            home_root.name = root_coll.name + "/home";
            home_root.parent_id = 1000;
            home_root.owner_name = std::string(admin_name);
            home_root.owner_zone = std::string(zone_name);
            if (auto ret = register_collection(home_root, out_cid); !ret.ok()) return ret;

            // 5. Register /zone/home/rods
            collection admin_home;
            admin_home.id = 1002;
            admin_home.name = home_root.name + "/" + std::string(admin_name);
            admin_home.parent_id = 1001;
            admin_home.owner_name = std::string(admin_name);
            admin_home.owner_zone = std::string(zone_name);
            if (auto ret = register_collection(admin_home, out_cid); !ret.ok()) return ret;

            return SUCCESS();
        }

        irods::error register_zone(const zone& z) {
            if (!engine_) return ERROR(-1, "Catalog not initialized");
            try {
                lite3cpp::Buffer buf;
                buf.init_object();
                buf.set_str(0, "name", z.name);
                buf.set_str(0, "type", z.type);
                buf.set_str(0, "connection", z.connection);
                buf.set_str(0, "comment", "");

                std::string node_id = "zone:" + z.name;
                engine_->put_node(node_id, buf.move_to_string());

                // MANDATORY INDICES
                lite3cpp::Buffer idx_buf;
                idx_buf.init_object();
                idx_buf.set_str(0, "id", node_id);
                engine_->put_node("idx:Zone:id:" + node_id, idx_buf.move_to_string());
                engine_->put_node("idx:Zone:name:" + z.name, idx_buf.move_to_string());

                return SUCCESS();
            } catch (const std::exception& e) {
                return ERROR(-1, e.what());
            }
        }

        irods::error modify_zone(std::string_view name, std::string_view prop, std::string_view value) {
            if (!engine_) return ERROR(-1, "Catalog not initialized");
            try {
                engine_->get_store()->patch_str("zone:" + std::string(name), std::string(prop), std::string(value));
                return SUCCESS();
            } catch (const std::exception& e) {
                return ERROR(-1, e.what());
            }
        }

        irods::error delete_zone(std::string_view name) {
            if (!engine_) return ERROR(-1, "Catalog not initialized");
            try {
                engine_->del_node("zone:" + std::string(name));
                return SUCCESS();
            } catch (const std::exception& e) {
                return ERROR(-1, e.what());
            }
        }

        irods::error get_next_sequence_value(std::string_view seq_name, uint64_t& out_val) {
            if (!engine_) return ERROR(-1, "Catalog not initialized");
            try {
                std::string seq_id = "seq:" + std::string(seq_name);
                auto node = engine_->get_node(seq_id);
                if (!node) {
                    lite3cpp::Buffer buf;
                    buf.init_object();
                    buf.set_i64(0, "value", 10000); 
                    engine_->put_node(seq_id, buf.move_to_string());
                    out_val = 10000;
                } else {
                    int64_t current = node->get_attribute<int64_t>("value");
                    out_val = static_cast<uint64_t>(current + 1);
                    engine_->get_store()->patch_int(seq_id, "value", static_cast<int64_t>(out_val));
                }
                return SUCCESS();
            } catch (const std::exception& e) {
                return ERROR(-1, e.what());
            }
        }

        irods::error register_data_object(const data_object& obj, data_id_t& out_id) {
            if (!engine_) return ERROR(-1, "Catalog not initialized");
            try {
                lite3cpp::Buffer buf;
                buf.init_object();
                buf.set_i64(0, "id", obj.id);
                buf.set_str(0, "name", obj.name);
                buf.set_i64(0, "size", obj.size);
                buf.set_str(0, "owner", obj.owner_name); // Mapped to 'owner'
                buf.set_str(0, "zone", obj.owner_zone); // Mapped to 'zone'
                buf.set_str(0, "create_ts", obj.create_ts);
                buf.set_str(0, "modify_ts", obj.modify_ts);
                buf.set_str(0, "checksum", obj.checksum);
                buf.set_i64(0, "coll_id", obj.coll_id);
                buf.set_str(0, "comment", "");

                std::string node_id = std::to_string(obj.id);
                engine_->put_node(node_id, buf.move_to_string());

                // MANDATORY INDICES
                lite3cpp::Buffer idx_buf;
                idx_buf.init_object();
                idx_buf.set_str(0, "id", node_id);
                engine_->put_node("idx:DataObject:id:" + node_id, idx_buf.move_to_string());
                engine_->put_node("idx:DataObject:name:" + obj.name, idx_buf.move_to_string());

                // Structural Edges
                lite3cpp::Buffer edge_buf;
                edge_buf.init_object();
                edge_buf.set_str(0, "name", obj.name);
                engine_->add_edge(std::to_string(obj.coll_id), "CONTAINS", 1.0, node_id, edge_buf.move_to_string());

                out_id = obj.id;
                return SUCCESS();
            } catch (const std::exception& e) {
                return ERROR(-1, e.what());
            }
        }

        irods::error delete_data_object(data_id_t id) {
            if (!engine_) return ERROR(-1, "Catalog not initialized");
            try {
                engine_->del_node(std::to_string(id));
                return SUCCESS();
            } catch (const std::exception& e) {
                return ERROR(-1, e.what());
            }
        }

        irods::error rename_data_object(data_id_t obj_id, std::string_view new_name) {
            if (!engine_) return ERROR(-1, "Catalog not initialized");
            try {
                engine_->get_store()->patch_str(std::to_string(obj_id), "name", std::string(new_name));
                return SUCCESS();
            } catch (const std::exception& e) {
                return ERROR(-1, e.what());
            }
        }

        irods::error move_data_object(data_id_t obj_id, coll_id_t target_coll_id) {
            if (!engine_) return ERROR(-1, "Catalog not initialized");
            try {
                engine_->get_store()->patch_int(std::to_string(obj_id), "coll_id", static_cast<int64_t>(target_coll_id));
                engine_->add_edge(std::to_string(target_coll_id), "CONTAINS", 1.0, std::to_string(obj_id), "{}");
                return SUCCESS();
            } catch (const std::exception& e) {
                return ERROR(-1, e.what());
            }
        }

        irods::error register_replica(const replica& repl) {
            if (!engine_) return ERROR(-1, "Catalog not initialized");
            try {
                std::string data_id_str = std::to_string(repl.data_id);
                std::string resc_id_str = std::to_string(repl.resource_id);
                std::string repl_node_id = "repl:" + data_id_str + ":" + std::to_string(repl.replica_number);

                lite3cpp::Buffer buf;
                buf.init_object();
                buf.set_i64(0, "data_id", repl.data_id);
                buf.set_i64(0, "repl_num", repl.replica_number);
                buf.set_str(0, "path", repl.physical_path);
                buf.set_str(0, "status", repl.status);
                buf.set_str(0, "checksum", repl.checksum);
                engine_->put_node(repl_node_id, buf.move_to_string());

                engine_->add_edge(data_id_str, "HAS_REPLICA", 1.0, repl_node_id, "{}");
                engine_->add_edge(repl_node_id, "STAYING_AT", 1.0, resc_id_str, "{}");

                return SUCCESS();
            } catch (const std::exception& e) {
                return ERROR(-1, e.what());
            }
        }

        irods::error register_collection(const collection& coll, coll_id_t& out_id) {
            if (!engine_) return ERROR(-1, "Catalog not initialized");
            try {
                lite3cpp::Buffer buf;
                buf.init_object();
                buf.set_i64(0, "id", coll.id);
                buf.set_str(0, "name", coll.name);
                buf.set_str(0, "owner", coll.owner_name); // Mapped to 'owner'
                buf.set_str(0, "owner_zone", coll.owner_zone);
                buf.set_str(0, "create_ts", "1715800000");
                buf.set_str(0, "modify_ts", "1715800000");
                buf.set_str(0, "type", ""); 
                buf.set_str(0, "comment", "");

                std::string node_id = std::to_string(coll.id);
                engine_->put_node(node_id, buf.move_to_string());

                // MANDATORY INDICES
                lite3cpp::Buffer idx_buf;
                idx_buf.init_object();
                idx_buf.set_str(0, "id", node_id);
                engine_->put_node("idx:Collection:id:" + node_id, idx_buf.move_to_string());
                engine_->put_node("idx:Collection:name:" + coll.name, idx_buf.move_to_string());

                // Structural Edges
                lite3cpp::Buffer edge_buf;
                edge_buf.init_object();
                edge_buf.set_str(0, "name", coll.name);
                if (coll.parent_id != 0) {
                    engine_->add_edge(std::to_string(coll.parent_id), "CONTAINS", 1.0, node_id, edge_buf.move_to_string());
                } else {
                    engine_->add_edge("zone:" + coll.owner_zone, "HAS_ROOT_COLL", 1.0, node_id, edge_buf.move_to_string());
                }

                out_id = coll.id;
                return SUCCESS();
            } catch (const std::exception& e) {
                return ERROR(-1, e.what());
            }
        }

        irods::error rename_collection(std::string_view old_name, std::string_view new_name) { return SUCCESS(); }
        irods::error delete_collection(coll_id_t coll_id) {
            if (!engine_) return ERROR(-1, "Catalog not initialized");
            try {
                engine_->del_node(std::to_string(coll_id));
                return SUCCESS();
            } catch (const std::exception& e) {
                return ERROR(-1, e.what());
            }
        }
        irods::error modify_collection(coll_id_t coll_id, std::string_view prop, std::string_view value) {
            if (!engine_) return ERROR(-1, "Catalog not initialized");
            try {
                engine_->get_store()->patch_str(std::to_string(coll_id), std::string(prop), std::string(value));
                return SUCCESS();
            } catch (const std::exception& e) {
                return ERROR(-1, e.what());
            }
        }

        irods::error register_resource(const resource& resc, resc_id_t& out_id) {
            if (!engine_) return ERROR(-1, "Catalog not initialized");
            try {
                lite3cpp::Buffer buf;
                buf.init_object();
                buf.set_i64(0, "id", resc.id);
                buf.set_str(0, "name", resc.name);
                buf.set_str(0, "type", resc.type);
                buf.set_str(0, "location", resc.location);
                buf.set_str(0, "vault_path", resc.vault_path);
                buf.set_str(0, "context", resc.context);
                buf.set_str(0, "comment", "");

                std::string node_id = std::to_string(resc.id);
                engine_->put_node(node_id, buf.move_to_string());

                // MANDATORY INDICES
                lite3cpp::Buffer idx_buf;
                idx_buf.init_object();
                idx_buf.set_str(0, "id", node_id);
                engine_->put_node("idx:Resource:id:" + node_id, idx_buf.move_to_string());
                engine_->put_node("idx:Resource:name:" + resc.name, idx_buf.move_to_string());

                out_id = resc.id;
                return SUCCESS();
            } catch (const std::exception& e) {
                return ERROR(-1, e.what());
            }
        }

        irods::error modify_resource(resc_id_t resc_id, std::string_view prop, std::string_view value) {
            if (!engine_) return ERROR(-1, "Catalog not initialized");
            try {
                engine_->get_store()->patch_str(std::to_string(resc_id), std::string(prop), std::string(value));
                return SUCCESS();
            } catch (const std::exception& e) {
                return ERROR(-1, e.what());
            }
        }

        irods::error delete_resource(resc_id_t resc_id) {
            if (!engine_) return ERROR(-1, "Catalog not initialized");
            try {
                engine_->del_node(std::to_string(resc_id));
                return SUCCESS();
            } catch (const std::exception& e) {
                return ERROR(-1, e.what());
            }
        }

        irods::error resolve_resource_name(std::string_view name, resc_id_t& out_id) {
            if (!engine_) return ERROR(-1, "Catalog not initialized");
            try {
                std::string idx_key = "idx:Resource:name:" + std::string(name);
                auto node = engine_->get_node(idx_key);
                if (!node || !node->has_attribute("id")) return ERROR(-1, "Resource not found");
                out_id = static_cast<resc_id_t>(std::stoll(node->get_attribute_as_string("id")));
                return SUCCESS();
            } catch (const std::exception& e) {
                return ERROR(-1, e.what());
            }
        }

        irods::error register_user(const user& usr, user_id_t& out_id) {
            if (!engine_) return ERROR(-1, "Catalog not initialized");
            try {
                lite3cpp::Buffer buf;
                buf.init_object();
                buf.set_i64(0, "id", usr.id);
                buf.set_str(0, "name", usr.name);
                buf.set_str(0, "zone", usr.zone);
                buf.set_str(0, "type", usr.type);
                int priv = (usr.type == "rodsadmin") ? 5 : 1;
                buf.set_i64(0, "priv_level", priv);
                buf.set_str(0, "comment", "");

                std::string node_id = "user:" + usr.name + "#" + usr.zone;
                engine_->put_node(node_id, buf.move_to_string());

                // MANDATORY INDICES
                lite3cpp::Buffer idx_buf;
                idx_buf.init_object();
                idx_buf.set_str(0, "id", node_id);
                engine_->put_node("idx:User:id:" + node_id, idx_buf.move_to_string());
                engine_->put_node("idx:User:name:" + usr.name, idx_buf.move_to_string());

                engine_->add_edge("zone:" + usr.zone, "HAS_USER", 1.0, node_id, "{}");

                out_id = usr.id;
                return SUCCESS();
            } catch (const std::exception& e) {
                return ERROR(-1, e.what());
            }
        }

        irods::error check_auth(std::string_view user_name, std::string_view zone, int& user_priv) {
            if (!engine_) return ERROR(-1, "Catalog not initialized");
            try {
                std::string node_id = "user:" + std::string(user_name) + "#" + std::string(zone);
                CAT_LOG(LOG_NOTICE, "L3KVG: check_auth searching for node [%s]", node_id.c_str());
                auto node = engine_->get_node(node_id);
                if (!node) return ERROR(-1, "User not found");

                if (node->has_attribute("priv_level")) {
                    user_priv = static_cast<int>(node->get_attribute<int64_t>("priv_level"));
                    CAT_LOG(LOG_NOTICE, "L3KVG: check_auth success for [%s], priv [%d]", node_id.c_str(), user_priv);
                } else {
                    return ERROR(-1, "priv_level attribute missing");
                }
                return SUCCESS();
            } catch (const std::exception& e) {
                return ERROR(-1, e.what());
            }
        }

        irods::error add_user_to_group(user_id_t user_id, user_id_t group_id) { return SUCCESS(); }
        irods::error remove_user_from_group(user_id_t user_id, user_id_t group_id) { return SUCCESS(); }
        irods::error set_user_property(user_id_t user_id, std::string_view prop, std::string_view value) { return SUCCESS(); }
        irods::error set_access(uint64_t user_id, uint64_t target_id, std::string_view level) { return SUCCESS(); }
        irods::error add_avu_metadata(std::string_view type, std::string_view target_id, const avu& metadata) { return SUCCESS(); }
        irods::error delete_avu_metadata(std::string_view type, std::string_view target_id, const avu& metadata) { return SUCCESS(); }
        irods::error modify_avu_metadata(std::string_view type, std::string_view target_id, const avu& old_avu, const avu& new_avu) { return SUCCESS(); }
        irods::error copy_avu_metadata(std::string_view src_type, std::string_view src_id, std::string_view dst_type, std::string_view dst_id) { return SUCCESS(); }
        irods::error set_avu_metadata(std::string_view type, std::string_view target_id, const avu& metadata) { return SUCCESS(); }
        irods::error add_metadata_with_acl(data_id_t object_id, const avu& metadata, const std::vector<uint64_t>& allowed_groups) { return SUCCESS(); }

        l3kvg::Engine* get_engine() { return engine_.get(); }

    private:
        std::unique_ptr<l3kvg::Engine> engine_;
    };

    CatalogFacade::CatalogFacade() : pImpl_(std::make_unique<CatalogImpl>()) {}
    CatalogFacade::~CatalogFacade() = default;

    irods::error CatalogFacade::init(const Config& cfg) { return pImpl_->init(cfg); }
    irods::error CatalogFacade::bootstrap_catalog(std::string_view zone_name, std::string_view admin_name) { return pImpl_->bootstrap_catalog(zone_name, admin_name); }
    irods::error CatalogFacade::register_zone(const zone& z) { return pImpl_->register_zone(z); }
    irods::error CatalogFacade::modify_zone(std::string_view name, std::string_view prop, std::string_view value) { return pImpl_->modify_zone(name, prop, value); }
    irods::error CatalogFacade::delete_zone(std::string_view name) { return pImpl_->delete_zone(name); }
    irods::error CatalogFacade::get_next_sequence_value(std::string_view seq_name, uint64_t& out_val) { return pImpl_->get_next_sequence_value(seq_name, out_val); }
    irods::error CatalogFacade::register_data_object(const data_object& obj, data_id_t& out_id) { return pImpl_->register_data_object(obj, out_id); }
    irods::error CatalogFacade::delete_data_object(data_id_t id) { return pImpl_->delete_data_object(id); }
    irods::error CatalogFacade::rename_data_object(data_id_t obj_id, std::string_view new_name) { return pImpl_->rename_data_object(obj_id, new_name); }
    irods::error CatalogFacade::move_data_object(data_id_t obj_id, coll_id_t target_coll_id) { return pImpl_->move_data_object(obj_id, target_coll_id); }
    irods::error CatalogFacade::register_replica(const replica& repl) { return pImpl_->register_replica(repl); }
    irods::error CatalogFacade::register_collection(const collection& coll, coll_id_t& out_id) { return pImpl_->register_collection(coll, out_id); }
    irods::error CatalogFacade::rename_collection(std::string_view old_name, std::string_view new_name) { return pImpl_->rename_collection(old_name, new_name); }
    irods::error CatalogFacade::delete_collection(coll_id_t coll_id) { return pImpl_->delete_collection(coll_id); }
    irods::error CatalogFacade::modify_collection(coll_id_t coll_id, std::string_view prop, std::string_view value) { return pImpl_->modify_collection(coll_id, prop, value); }
    irods::error CatalogFacade::register_resource(const resource& resc, resc_id_t& out_id) { return pImpl_->register_resource(resc, out_id); }
    irods::error CatalogFacade::modify_resource(resc_id_t resc_id, std::string_view prop, std::string_view value) { return pImpl_->modify_resource(resc_id, prop, value); }
    irods::error CatalogFacade::delete_resource(resc_id_t resc_id) { return pImpl_->delete_resource(resc_id); }
    irods::error CatalogFacade::resolve_resource_name(std::string_view name, resc_id_t& out_id) { return pImpl_->resolve_resource_name(name, out_id); }
    irods::error CatalogFacade::register_user(const user& usr, user_id_t& out_id) { return pImpl_->register_user(usr, out_id); }
    irods::error CatalogFacade::check_auth(std::string_view user_name, std::string_view zone, int& user_priv) { return pImpl_->check_auth(user_name, zone, user_priv); }
    irods::error CatalogFacade::add_user_to_group(user_id_t user_id, user_id_t group_id) { return pImpl_->add_user_to_group(user_id, group_id); }
    irods::error CatalogFacade::remove_user_from_group(user_id_t user_id, user_id_t group_id) { return pImpl_->remove_user_from_group(user_id, group_id); }
    irods::error CatalogFacade::set_user_property(user_id_t user_id, std::string_view prop, std::string_view value) { return pImpl_->set_user_property(user_id, prop, value); }
    irods::error CatalogFacade::set_access(uint64_t user_id, uint64_t target_id, std::string_view level) { return pImpl_->set_access(user_id, target_id, level); }
    irods::error CatalogFacade::add_avu_metadata(std::string_view type, std::string_view target_id, const avu& metadata) { return pImpl_->add_avu_metadata(type, target_id, metadata); }
    irods::error CatalogFacade::delete_avu_metadata(std::string_view type, std::string_view target_id, const avu& metadata) { return pImpl_->delete_avu_metadata(type, target_id, metadata); }
    irods::error CatalogFacade::modify_avu_metadata(std::string_view type, std::string_view target_id, const avu& old_avu, const avu& new_avu) { return pImpl_->modify_avu_metadata(type, target_id, old_avu, new_avu); }
    irods::error CatalogFacade::copy_avu_metadata(std::string_view src_type, std::string_view src_id, std::string_view dst_type, std::string_view dst_id) { return pImpl_->copy_avu_metadata(src_type, src_id, dst_type, dst_id); }
    irods::error CatalogFacade::set_avu_metadata(std::string_view type, std::string_view target_id, const avu& metadata) { return pImpl_->set_avu_metadata(type, target_id, metadata); }
    irods::error CatalogFacade::add_metadata_with_acl(data_id_t object_id, const avu& metadata, const std::vector<uint64_t>& allowed_groups) { return pImpl_->add_metadata_with_acl(object_id, metadata, allowed_groups); }
    irods::error CatalogFacade::execute_query(const irods::experimental::genquery2::select& ast, ResultSet& results) { return pImpl_->execute_query(ast, results); }
    l3kvg::Engine* CatalogFacade::get_engine() { return pImpl_->get_engine(); }

} // namespace irods::catalog
