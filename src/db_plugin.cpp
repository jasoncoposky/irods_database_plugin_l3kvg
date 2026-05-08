#include "irods/irods_database_plugin.hpp"
#include "irods/irods_database_constants.hpp"
#include "irods/catalog/catalog_facade.hpp"
#include "irods/catalog/catalog_schemas.hpp"
#include <memory>

namespace {
    std::unique_ptr<irods::catalog::CatalogFacade> g_catalog;
}

// Helper to convert legacy iRODS structs to modern catalog models
// In a real implementation, this would use the variadic mappers we built.

irods::error db_reg_data_obj_op(irods::plugin_context& _ctx, dataObjInfo_t* _info) {
    if (!_info) return ERROR(SYS_INVALID_INPUT_PARAM, "null dataObjInfo_t");
    
    irods::catalog::data_object obj;
    obj.id = _info->dataId;
    obj.coll_id = _info->collId;
    obj.name = _info->objPath;
    obj.size = _info->dataSize;

    obj.owner_name = _info->dataOwnerName;
    obj.create_ts = _info->dataCreate;
    obj.modify_ts = _info->dataModify;

    irods::catalog::data_id_t out_id;
    auto ret = g_catalog->register_data_object(obj, out_id);
    if (ret.ok()) {
        _info->dataId = out_id;
    }
    return ret;
}

irods::error db_reg_replica_op(irods::plugin_context& _ctx, dataObjInfo_t* _src, dataObjInfo_t* _dst, keyValPair_t* _cond) {
    if (!_dst) return ERROR(SYS_INVALID_INPUT_PARAM, "null destination dataObjInfo_t");

    irods::catalog::replica repl;
    repl.data_id = _dst->dataId;
    repl.resource_id = _dst->rescId;
    repl.physical_path = _dst->filePath;
    repl.replica_number = _dst->replNum;
    repl.status = _dst->statusString;

    return g_catalog->register_replica(repl);
}

irods::error db_mod_access_control_op(irods::plugin_context& _ctx, int _recursive, const char* _level, const char* _user, const char* _zone, const char* _path) {
    // In L3KVG, we'd look up the user_id and target_id (path_id) first.
    // For this prototype, we'll assume we have them or can resolve them.
    return g_catalog->set_access(123, 456, _level); // Placeholder IDs
}

irods::error db_start_op(irods::plugin_context& _ctx) {
    g_catalog = std::make_unique<irods::catalog::CatalogFacade>();
    return SUCCESS();
}

irods::error db_close_op(irods::plugin_context& _ctx) {
    g_catalog.reset();
    return SUCCESS();
}

class l3kvg_database_plugin : public irods::database {
public:
    l3kvg_database_plugin(const std::string& _inst, const std::string& _ctx)
        : irods::database(_inst, _ctx) {
        
        add_operation(irods::DATABASE_OP_START, std::function<irods::error(irods::plugin_context&)>(db_start_op));
        add_operation(irods::DATABASE_OP_CLOSE, std::function<irods::error(irods::plugin_context&)>(db_close_op));
        
        add_operation<dataObjInfo_t*>(
            irods::DATABASE_OP_REG_DATA_OBJ,
            std::function<irods::error(irods::plugin_context&, dataObjInfo_t*)>(db_reg_data_obj_op));
            
        add_operation<dataObjInfo_t*, dataObjInfo_t*, keyValPair_t*>(
            irods::DATABASE_OP_REG_REPLICA,
            std::function<irods::error(irods::plugin_context&, dataObjInfo_t*, dataObjInfo_t*, keyValPair_t*)>(db_reg_replica_op));

        add_operation<int, const char*, const char*, const char*, const char*>(
            irods::DATABASE_OP_MOD_ACCESS_CONTROL,
            std::function<irods::error(irods::plugin_context&, int, const char*, const char*, const char*, const char*)>(db_mod_access_control_op));
    }
};

extern "C" irods::database* plugin_factory(const std::string& _inst_name, const std::string& _context) {
    return new l3kvg_database_plugin(_inst_name, _context);
}
