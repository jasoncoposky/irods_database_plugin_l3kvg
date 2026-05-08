#include "irods/catalog/catalog_facade.hpp"
#include "L3KVG/Engine.hpp"
#include "L3KVG/Node.hpp"
#include "engine/store.hpp"
#include "irods/catalog/l3kvg_mapper.hpp"
#include "irods/catalog/catalog_schemas.hpp"
#include "irods/catalog/gq2_compiler.hpp"
#include <iostream>

namespace irods::catalog {

    class CatalogImpl {
    public:
        CatalogImpl() = default;

        irods::error init(const Config& cfg) {
            try {
                if (engine_) {
                    return ERROR(-1, "Catalog already initialized");
                }
                // In a real iRODS 5 deployment, we'd use cfg.shard_count and cfg.zmq_endpoint
                // L3KVG Engine constructor might need updates to support these.
                engine_ = std::make_unique<l3kvg::Engine>(cfg.db_path, cfg.node_id);
                return SUCCESS();
            } catch (const std::exception& e) {
                return ERROR(-1, e.what());
            }
        }

        irods::error execute_query(const std::vector<compiler::AstNode>& ast, ResultSet& results) {
            if (!engine_) return ERROR(-1, "Catalog not initialized");
            try {
                compiler::Gq2ToL3kvgCompiler compiler(*engine_);
                auto query = compiler.compile(ast);
                results.rows = query.execute(); // Move rows into ResultSet
                return SUCCESS();
            } catch (const std::exception& e) {
                return ERROR(-1, e.what());
            }
        }

        irods::error register_data_object(const data_object& obj, data_id_t& out_id) {
            if (!engine_) return ERROR(-1, "Catalog not initialized");
            try {
                // 1. Serialize to BSON using our zero-copy template
                // Note: We use the schema mapping to build the buffer directly from the struct
                // For this prototype we'll use a manually populated buffer but via to_view
                lite3cpp::Buffer buf;
                buf.init_object();
                buf.set_i64(0, "id", obj.id);
                buf.set_str(0, "name", obj.name);
                buf.set_i64(0, "size", obj.size);
                buf.set_str(0, "owner", obj.owner_name);
                buf.set_str(0, "create_ts", obj.create_ts);
                buf.set_str(0, "modify_ts", obj.modify_ts);

                std::string node_id = std::to_string(obj.id);
                
                // 2. Write to Graph Fabric (using move ownership)
                engine_->put_node(std::move(node_id), buf.move_to_string());

                // 3. Link to Parent Collection
                engine_->add_edge(std::to_string(obj.coll_id), "CONTAINS", 1.0, std::to_string(obj.id), "{}");

                out_id = obj.id;
                return SUCCESS();
            } catch (const std::exception& e) {
                return ERROR(-1, e.what());
            }
        }

        irods::error delete_data_object(data_id_t id) {
            if (!engine_) return ERROR(-1, "Catalog not initialized");
            // L3KVG doesn't have a direct delete_node in Engine.hpp yet, 
            // but we'd implement cascading delete here.
            return SUCCESS();
        }

        irods::error register_replica(const replica& repl) {
            if (!engine_) return ERROR(-1, "Catalog not initialized");
            try {
                lite3cpp::Buffer props;
                props.init_object();
                props.set_str(0, "path", repl.physical_path);
                props.set_i64(0, "repl_num", repl.replica_number);
                props.set_str(0, "status", repl.status);

                engine_->add_edge(
                    std::to_string(repl.data_id),
                    "REPLICATED_ON",
                    1.0,
                    std::to_string(repl.resource_id),
                    props.move_to_string()
                );
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
                
                std::string node_id = std::to_string(coll.id);
                engine_->put_node(std::move(node_id), buf.move_to_string());

                if (coll.parent_id != 0) {
                    engine_->add_edge(std::to_string(coll.parent_id), "CONTAINS", 1.0, std::to_string(coll.id), "{}");
                }

                out_id = coll.id;
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

                // Map 'type' to numeric privilege level for db_check_auth
                int priv = (usr.type == "rodsadmin") ? 5 : 1;
                buf.set_i64(0, "priv_level", priv);

                std::string node_id = "user:" + usr.name + "#" + usr.zone;
                engine_->put_node(std::move(node_id), buf.move_to_string());

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
                auto node = engine_->get_node(node_id);
                if (!node) return ERROR(-1, "User not found");

                // Check for attribute existence and retrieve value
                if (node->has_attribute("priv_level")) {
                    // Try to get as Int64 (Native BSON)
                    user_priv = static_cast<int>(node->get_attribute<int64_t>("priv_level"));
                } else {
                    return ERROR(-1, "priv_level attribute missing on user node");
                }
                
                return SUCCESS();
            } catch (const std::exception& e) {
                return ERROR(-1, e.what());
            }
        }

        irods::error add_user_to_group(user_id_t user_id, user_id_t group_id) {
            if (!engine_) return ERROR(-1, "Catalog not initialized");
            try {
                engine_->add_edge(std::to_string(user_id), "MEMBER_OF", 1.0, std::to_string(group_id), "{}");
                return SUCCESS();
            } catch (const std::exception& e) {
                return ERROR(-1, e.what());
            }
        }

        irods::error remove_user_from_group(user_id_t user_id, user_id_t group_id) {
            if (!engine_) return ERROR(-1, "Catalog not initialized");
            // L3KVG needs delete_edge support.
            return SUCCESS();
        }

        irods::error set_user_property(user_id_t user_id, std::string_view prop, std::string_view value) {
            if (!engine_) return ERROR(-1, "Catalog not initialized");
            try {
                // In L3KVG, we use the Patch API for zero-copy attribute updates
                // We bypass loading the whole node.
                engine_->get_store()->patch_str(std::to_string(user_id), std::string(prop), std::string(value));
                return SUCCESS();
            } catch (const std::exception& e) {
                return ERROR(-1, e.what());
            }
        }

        irods::error set_access(uint64_t user_id, uint64_t target_id, std::string_view level) {
            if (!engine_) return ERROR(-1, "Catalog not initialized");
            try {
                lite3cpp::Buffer props;
                props.init_object();
                props.set_str(0, "level", level);

                engine_->add_edge(
                    std::to_string(user_id),
                    "HAS_ACCESS",
                    1.0,
                    std::to_string(target_id),
                    props.move_to_string()
                );
                return SUCCESS();
            } catch (const std::exception& e) {
                return ERROR(-1, e.what());
            }
        }

        // New: Metadata-specific ACL pushdown
        irods::error add_metadata_with_acl(data_id_t object_id, const avu& metadata, const std::vector<uint64_t>& allowed_groups) {
            if (!engine_) return ERROR(-1, "Catalog not initialized");
            try {
                // 1. Create the AVU Node
                lite3cpp::Buffer buf;
                buf.init_object();
                buf.set_str(0, "attr", metadata.attribute);
                buf.set_str(0, "val", metadata.value);
                
                std::string avu_id = metadata.attribute + ":" + metadata.value; // Unique ID
                engine_->put_node(avu_id, buf.move_to_string());

                // 2. Create the ANNOTATED_WITH edge with the "Fat Payload" for Pushdown
                lite3cpp::Buffer edge_props;
                edge_props.init_object();
                
                // Consistency: Store a list of group/user IDs that can see this metadata
                if (!allowed_groups.empty()) {
                    size_t acl_ofs = edge_props.set_arr(0, "acl");
                    for (uint64_t gid : allowed_groups) {
                        edge_props.arr_append_i64(acl_ofs, static_cast<int64_t>(gid));
                    }
                }

                engine_->add_edge(
                    std::to_string(object_id),
                    "ANNOTATED_WITH",
                    1.0,
                    avu_id,
                    edge_props.move_to_string()
                );

                return SUCCESS();
            } catch (const std::exception& e) {
                return ERROR(-1, e.what());
            }
        }

    private:
        std::unique_ptr<l3kvg::Engine> engine_;
    };

    CatalogFacade::CatalogFacade() : pImpl_(std::make_unique<CatalogImpl>()) {}
    CatalogFacade::~CatalogFacade() = default;

    irods::error CatalogFacade::init(const Config& cfg) {
        return pImpl_->init(cfg);
    }

    irods::error CatalogFacade::register_data_object(const data_object& obj, data_id_t& out_id) {
        return pImpl_->register_data_object(obj, out_id);
    }

    irods::error CatalogFacade::delete_data_object(data_id_t id) {
        return pImpl_->delete_data_object(id);
    }

    irods::error CatalogFacade::register_replica(const replica& repl) {
        return pImpl_->register_replica(repl);
    }

    irods::error CatalogFacade::register_collection(const collection& coll, coll_id_t& out_id) {
        return pImpl_->register_collection(coll, out_id);
    }

    irods::error CatalogFacade::register_user(const user& usr, user_id_t& out_id) {
        return pImpl_->register_user(usr, out_id);
    }

    irods::error CatalogFacade::check_auth(std::string_view user_name, std::string_view zone, int& user_priv) {
        return pImpl_->check_auth(user_name, zone, user_priv);
    }

    irods::error CatalogFacade::add_user_to_group(user_id_t user_id, user_id_t group_id) {
        return pImpl_->add_user_to_group(user_id, group_id);
    }

    irods::error CatalogFacade::remove_user_from_group(user_id_t user_id, user_id_t group_id) {
        return pImpl_->remove_user_from_group(user_id, group_id);
    }

    irods::error CatalogFacade::set_user_property(user_id_t user_id, std::string_view prop, std::string_view value) {
        return pImpl_->set_user_property(user_id, prop, value);
    }

    irods::error CatalogFacade::set_access(uint64_t user_id, uint64_t target_id, std::string_view level) {
        return pImpl_->set_access(user_id, target_id, level);
    }

    irods::error CatalogFacade::add_metadata_with_acl(data_id_t object_id, const avu& metadata, const std::vector<uint64_t>& allowed_groups) {
        return pImpl_->add_metadata_with_acl(object_id, metadata, allowed_groups);
    }

    irods::error CatalogFacade::execute_query(const std::vector<compiler::AstNode>& ast, ResultSet& results) {
        return pImpl_->execute_query(ast, results);
    }

} // namespace irods::catalog
