#include "irods/catalog/catalog_facade.hpp"
#include "L3KVG/Engine.hpp"
#include "irods/catalog/l3kvg_mapper.hpp"
#include "irods/catalog/catalog_schemas.hpp"
#include <iostream>

namespace irods::catalog {

    class CatalogImpl {
    public:
        CatalogImpl() {
            // In a real iRODS 5 deployment, the path and node_id would come from server_config.json
            engine_ = std::make_unique<l3kvg::Engine>("/var/lib/irods/catalog.l3kvg", 1);
        }

        irods::error register_data_object(const data_object& obj, data_id_t& out_id) {
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
            // L3KVG doesn't have a direct delete_node in Engine.hpp yet, 
            // but we'd implement cascading delete here.
            return SUCCESS();
        }

        irods::error register_replica(const replica& repl) {
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

        irods::error set_access(uint64_t user_id, uint64_t target_id, std::string_view level) {
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

    private:
        std::unique_ptr<l3kvg::Engine> engine_;
    };

    CatalogFacade::CatalogFacade() : pImpl_(std::make_unique<CatalogImpl>()) {}
    CatalogFacade::~CatalogFacade() = default;

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

    irods::error CatalogFacade::set_access(uint64_t user_id, uint64_t target_id, std::string_view level) {
        return pImpl_->set_access(user_id, target_id, level);
    }

} // namespace irods::catalog
