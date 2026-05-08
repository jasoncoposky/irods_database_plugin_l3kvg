#include "irods/irods_database_plugin.hpp"
#include "irods/irods_database_constants.hpp"
#include "irods/irods_server_properties.hpp"
#include "irods/irods_configuration_keywords.hpp"
#include "irods/catalog/catalog_facade.hpp"
#include "irods/catalog/catalog_schemas.hpp"
#include "irods/catalog/gq2_compiler.hpp"
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
    if (g_catalog) return SUCCESS();

    try {
        const auto& config_handle{irods::server_properties::instance().map()};
        const auto& config_json{config_handle.get_json()};

        // Navigate to: plugin_configuration.database.plugin_specific_configuration
        if (!config_json.contains(irods::KW_CFG_PLUGIN_CONFIGURATION) ||
            !config_json.at(irods::KW_CFG_PLUGIN_CONFIGURATION).contains(irods::KW_CFG_PLUGIN_TYPE_DATABASE)) {
            return ERROR(SYS_CONFIG_FILE_ERR, "Missing database plugin configuration");
        }

        const auto& db_config = config_json.at(irods::KW_CFG_PLUGIN_CONFIGURATION).at(irods::KW_CFG_PLUGIN_TYPE_DATABASE);
        
        if (!db_config.contains(irods::KW_CFG_PLUGIN_SPECIFIC_CONFIGURATION)) {
            return ERROR(SYS_CONFIG_FILE_ERR, "Missing plugin_specific_configuration for L3KVG");
        }

        const auto& spec_config = db_config.at(irods::KW_CFG_PLUGIN_SPECIFIC_CONFIGURATION);

        irods::catalog::Config cfg;
        
        // Rigorous validation to avoid INVALID_ANY_CAST or crashes
        if (!spec_config.contains("db_path") || !spec_config.at("db_path").is_string()) {
            return ERROR(SYS_INVALID_INPUT_PARAM, "L3KVG 'db_path' missing or not a string");
        }
        cfg.db_path = spec_config.at("db_path").get<std::string>();

        if (!spec_config.contains("node_id") || !spec_config.at("node_id").is_number()) {
            return ERROR(SYS_INVALID_INPUT_PARAM, "L3KVG 'node_id' missing or not a number");
        }
        cfg.node_id = spec_config.at("node_id").get<uint32_t>();

        // Optional settings with defaults
        cfg.shard_count = spec_config.contains("shard_count") ? spec_config.at("shard_count").get<uint32_t>() : 64;
        cfg.zmq_endpoint = spec_config.contains("zmq_endpoint") ? spec_config.at("zmq_endpoint").get<std::string>() : "tcp://127.0.0.1:5555";

        g_catalog = std::make_unique<irods::catalog::CatalogFacade>();
        return g_catalog->init(cfg);

    } catch (const std::exception& e) {
        return ERROR(SYS_CONFIG_FILE_ERR, e.what());
    }
}

irods::error db_close_op(irods::plugin_context& _ctx) {
    g_catalog.reset();
    return SUCCESS();
}

irods::error db_gen_query_op(irods::plugin_context& _ctx, genQueryInp_t* _inp, genQueryOut_t* _out) {
    if (!_inp || !_out) return ERROR(SYS_INVALID_INPUT_PARAM, "null genQueryInp_t/Out_t");

    // 1. Translate genQueryInp_t to our AST model
    std::vector<irods::catalog::compiler::AstNode> ast;
    
    // Process SELECT columns
    irods::catalog::compiler::SelectNode sel;
    for (int i = 0; i < _inp->selectInp.len; ++i) {
        sel.columns.push_back(_inp->selectInp.inx[i]);
    }
    ast.push_back(std::move(sel));

    // Process WHERE conditions
    for (int i = 0; i < _inp->sqlCondInp.len; ++i) {
        irods::catalog::compiler::ConditionNode cond;
        cond.column = _inp->sqlCondInp.inx[i];
        cond.value = _inp->sqlCondInp.value[i];
        cond.op = "="; // Simplified for prototype
        ast.push_back(std::move(cond));
    }

    // 2. Execute via Facade
    std::vector<std::vector<std::string>> results;
    auto ret = g_catalog->execute_query(ast, results);
    if (!ret.ok()) return ret;

    // 3. Map results back to genQueryOut_t
    _out->rowCnt = static_cast<int>(results.size());
    _out->attriCnt = static_cast<int>(_inp->selectInp.len);
    
    for (int j = 0; j < _out->attriCnt; ++j) {
        _out->sqlResult[j].attriInx = _inp->selectInp.inx[j];
        
        // Calculate max len for this column
        size_t max_len = 0;
        for (const auto& row : results) {
            if (row[j].length() > max_len) max_len = row[j].length();
        }
        _out->sqlResult[j].len = static_cast<int>(max_len + 1);
        _out->sqlResult[j].value = (char*)malloc(_out->rowCnt * _out->sqlResult[j].len);
        
        for (int i = 0; i < _out->rowCnt; ++i) {
            std::strncpy(&_out->sqlResult[j].value[i * _out->sqlResult[j].len], results[i][j].c_str(), _out->sqlResult[j].len);
        }
    }

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

        add_operation<genQueryInp_t*, genQueryOut_t*>(
            irods::DATABASE_OP_GEN_QUERY,
            std::function<irods::error(irods::plugin_context&, genQueryInp_t*, genQueryOut_t*)>(db_gen_query_op));
    }
};

extern "C" irods::database* plugin_factory(const std::string& _inst_name, const std::string& _context) {
    return new l3kvg_database_plugin(_inst_name, _context);
}
