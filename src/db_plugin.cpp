#include "irods/irods_database_plugin.hpp"
#include "irods/irods_database_constants.hpp"
#include "irods/irods_server_properties.hpp"
#include "irods/irods_configuration_keywords.hpp"
#include "irods/catalog/catalog_facade.hpp"
#include "irods/catalog/gq2_compiler.hpp"
#include "irods/private/genquery2_driver.hpp"
#include "L3KVG/Node.hpp"
#include "irods/rodsLog.h"
#include "irods/rodsErrorTable.h"
#include "irods/objInfo.h"
#include "irods/rsGenQuery.hpp"
#include "irods/rcMisc.h"

#include <memory>
#include <string>
#include <vector>
#include <map>
#include <fstream>
#include <regex>

// Define missing keywords if not in headers
#ifndef KW_CFG_PLUGIN_CONFIGURATION
#define KW_CFG_PLUGIN_CONFIGURATION "plugin_configuration"
#endif
#ifndef KW_CFG_PLUGIN_TYPE_DATABASE
#define KW_CFG_PLUGIN_TYPE_DATABASE "database"
#endif
#ifndef KW_CFG_ZONE_NAME
#define KW_CFG_ZONE_NAME "zone_name"
#endif
#ifndef KW_CFG_ZONE_USER
#define KW_CFG_ZONE_USER "zone_user"
#endif

static std::unique_ptr<irods::catalog::CatalogFacade> g_catalog;

// Common initialization logic used by both maintenance start and context-based start
irods::error init_l3kvg_catalog() {
    if (g_catalog) {
        return SUCCESS();
    }

    try {
        rodsLog(LOG_NOTICE, "L3_PLUGIN: Initializing L3KVG Catalog");
        const auto& config_handle{irods::server_properties::instance().map()};
        const auto& config_json{config_handle.get_json()};
        
        if (!config_json.contains(KW_CFG_PLUGIN_CONFIGURATION) ||
            !config_json.at(KW_CFG_PLUGIN_CONFIGURATION).contains(KW_CFG_PLUGIN_TYPE_DATABASE)) {
            rodsLog(LOG_ERROR, "L3_PLUGIN: ERROR - Missing plugin_configuration or database");
            return ERROR(SYS_CONFIG_FILE_ERR, "Missing L3KVG configuration");
        }

        const auto& db_config = config_json.at(KW_CFG_PLUGIN_CONFIGURATION).at(KW_CFG_PLUGIN_TYPE_DATABASE);
        
        // Find our config - could be directly under "database", or under "l3kvg"/"postgres" sub-keys
        nlohmann::json spec_config;
        if (db_config.contains(irods::KW_CFG_PLUGIN_SPECIFIC_CONFIGURATION)) {
            spec_config = db_config.at(irods::KW_CFG_PLUGIN_SPECIFIC_CONFIGURATION);
        } else if (db_config.contains("l3kvg") && db_config.at("l3kvg").contains(irods::KW_CFG_PLUGIN_SPECIFIC_CONFIGURATION)) {
            spec_config = db_config.at("l3kvg").at(irods::KW_CFG_PLUGIN_SPECIFIC_CONFIGURATION);
        } else if (db_config.contains("postgres") && db_config.at("postgres").contains(irods::KW_CFG_PLUGIN_SPECIFIC_CONFIGURATION)) {
             spec_config = db_config.at("postgres").at(irods::KW_CFG_PLUGIN_SPECIFIC_CONFIGURATION);
        } else {
            rodsLog(LOG_ERROR, "L3_PLUGIN: ERROR - No specific configuration found for l3kvg or postgres");
            return ERROR(SYS_CONFIG_FILE_ERR, "Missing L3KVG specific configuration");
        }

        irods::catalog::Config cfg;
        cfg.db_path = spec_config.at("db_path").get<std::string>();
        cfg.node_id = spec_config.at("node_id").get<uint32_t>();
        cfg.shard_count = spec_config.contains("shard_count") ? spec_config.at("shard_count").get<uint32_t>() : 64;
        cfg.zmq_endpoint = spec_config.contains("zmq_endpoint") ? spec_config.at("zmq_endpoint").get<std::string>() : "tcp://127.0.0.1:5555";

        rodsLog(LOG_NOTICE, "L3_PLUGIN: Initializing CatalogFacade with path [%s]", cfg.db_path.c_str());
        g_catalog = std::make_unique<irods::catalog::CatalogFacade>();
        if (auto ret = g_catalog->init(cfg); !ret.ok()) {
            rodsLog(LOG_ERROR, "L3_PLUGIN: ERROR - CatalogFacade init failed: %s", ret.result().c_str());
            return ret;
        }

        const std::string& zone_name = config_json.at(KW_CFG_ZONE_NAME).get<std::string>();
        const std::string& admin_name = config_json.at(KW_CFG_ZONE_USER).get<std::string>();
        
        rodsLog(LOG_NOTICE, "L3_PLUGIN: Bootstrapping catalog for zone [%s] and admin [%s]", zone_name.c_str(), admin_name.c_str());
        auto ret = g_catalog->bootstrap_catalog(zone_name, admin_name);
        if (!ret.ok()) {
            rodsLog(LOG_ERROR, "L3_PLUGIN: Bootstrap failed: %s", ret.result().c_str());
        }

        // Diagnostic check
        if (g_catalog->get_engine()) {
             auto node = g_catalog->get_engine()->get_node("user:" + admin_name + "#" + zone_name);
             if (node) {
                 rodsLog(LOG_NOTICE, "L3_PLUGIN: Bootstrap Verified - user node [%s] exists", node->get_uuid().c_str());
             } else {
                 rodsLog(LOG_ERROR, "L3_PLUGIN: Bootstrap FAILED - user node not found!");
             }
        }

        return ret;

    } catch (const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXCEPTION in init_l3kvg_catalog: %s", e.what());
        return ERROR(SYS_CONFIG_FILE_ERR, e.what());
    }
}

irods::error db_start_operation(irods::plugin_property_map& _props) {
    rodsLog(LOG_NOTICE, "L3_PLUGIN: db_start_operation (maintenance) called");
    return init_l3kvg_catalog();
}

irods::error db_start_op(irods::plugin_context& _ctx) {
    rodsLog(LOG_NOTICE, "L3_PLUGIN: db_start_op (context) called");
    return init_l3kvg_catalog();
}

irods::error db_open_op(irods::plugin_context& _ctx) {
    rodsLog(LOG_NOTICE, "L3_PLUGIN: db_open_op called");
    return SUCCESS();
}

irods::error db_close_op(irods::plugin_context& _ctx) {
    rodsLog(LOG_NOTICE, "L3_PLUGIN: db_close_op called");
    g_catalog.reset();
    return SUCCESS();
}

irods::error db_reg_data_obj_op(irods::plugin_context& _ctx, dataObjInfo_t* _info) {
    rodsLog(LOG_NOTICE, "L3_PLUGIN: db_reg_data_obj_op called for [%s]", _info->objPath);
    if (!_info) return ERROR(SYS_INVALID_INPUT_PARAM, "null dataObjInfo_t");
    
    irods::catalog::data_object obj;
    obj.id = _info->dataId;
    obj.coll_id = _info->collId;
    obj.name = _info->objPath;
    obj.size = _info->dataSize;
    obj.owner_name = _info->dataOwnerName;
    obj.owner_zone = _info->dataOwnerZone;
    obj.create_ts = _info->dataCreate;
    obj.modify_ts = _info->dataModify;
    obj.checksum = _info->chksum;
    obj.repl_num = (uint32_t)_info->replNum;
    obj.resc_name = _info->rescName;
    obj.path = _info->filePath;
    obj.resc_hier = _info->rescHier;
    obj.resc_id = _info->rescId;
    obj.repl_status = _info->statusString;

    if (obj.id <= 0) {
        if (auto ret = g_catalog->get_next_sequence_value("R_DATA_MAIN", obj.id); !ret.ok()) {
            return ret;
        }
    }

    irods::catalog::data_id_t out_id;
    auto ret = g_catalog->register_data_object(obj, out_id);
    if (ret.ok()) {
        _info->dataId = out_id;
    }
    return ret;
}

irods::error db_reg_replica_op(irods::plugin_context& _ctx, dataObjInfo_t* _src, dataObjInfo_t* _dst, keyValPair_t* _cond) {
    rodsLog(LOG_NOTICE, "L3_PLUGIN: db_reg_replica_op called for [%s] to [%s]", _dst->objPath, _dst->rescName);
    if (!_dst) return ERROR(SYS_INVALID_INPUT_PARAM, "null destination dataObjInfo_t");

    irods::catalog::replica repl;
    repl.data_id = _dst->dataId;
    repl.resource_id = _dst->rescId;
    repl.physical_path = _dst->filePath;
    repl.replica_number = (uint32_t)_dst->replNum;
    repl.status = _dst->statusString;
    repl.checksum = _dst->chksum;

    return g_catalog->register_replica(repl);
}

static std::string get_column_name_v4(int inx) {
    switch (inx) {
        case COL_ZONE_ID: return "ZONE_ID";
        case COL_ZONE_NAME: return "ZONE_NAME";
        case COL_ZONE_TYPE: return "ZONE_TYPE";
        case COL_ZONE_CONNECTION: return "ZONE_CONNECTION";
        case COL_ZONE_COMMENT: return "ZONE_COMMENT";
        case COL_USER_ID: return "USER_ID";
        case COL_USER_NAME: return "USER_NAME";
        case COL_USER_TYPE: return "USER_TYPE";
        case COL_USER_ZONE: return "USER_ZONE";
        case COL_R_RESC_ID: return "RESC_ID";
        case COL_R_RESC_NAME: return "RESC_NAME";
        case COL_R_ZONE_NAME: return "R_ZONE_NAME";
        case COL_R_TYPE_NAME: return "RESC_TYPE_NAME";
        case COL_R_LOC: return "RESC_LOC";
        case COL_R_VAULT_PATH: return "RESC_VAULT_PATH";
        case COL_R_FREE_SPACE: return "RESC_FREE_SPACE";
        case COL_D_DATA_ID: return "DATA_ID";
        case COL_D_COLL_ID: return "D_COLL_ID";
        case COL_DATA_NAME: return "DATA_NAME";
        case COL_DATA_REPL_NUM: return "DATA_REPL_NUM";
        case COL_DATA_SIZE: return "DATA_SIZE";
        case COL_D_RESC_NAME: return "D_RESC_NAME";
        case COL_D_DATA_PATH: return "DATA_PATH";
        case COL_D_OWNER_NAME: return "DATA_OWNER_NAME";
        case COL_D_OWNER_ZONE: return "DATA_OWNER_ZONE";
        case COL_D_CREATE_TIME: return "DATA_CREATE_TIME";
        case COL_D_MODIFY_TIME: return "DATA_MODIFY_TIME";
        case COL_D_RESC_ID: return "DATA_RESC_ID";
        case COL_COLL_ID: return "COLL_ID";
        case COL_COLL_NAME: return "COLL_NAME";
        case COL_COLL_PARENT_NAME: return "COLL_PARENT_NAME";
        case COL_COLL_OWNER_NAME: return "COLL_OWNER_NAME";
        case COL_COLL_OWNER_ZONE: return "COLL_OWNER_ZONE";
        case COL_COLL_CREATE_TIME: return "COLL_CREATE_TIME";
        case COL_COLL_MODIFY_TIME: return "COLL_MODIFY_TIME";
        case COL_COLL_TYPE: return "COLL_TYPE";
        case COL_COLL_INFO1: return "COLL_COMMENT";
        case COL_COLL_INFO2: return "COLL_COMMENT";
        case COL_META_DATA_ATTR_NAME: return "META_DATA_ATTR_NAME";
        case COL_META_DATA_ATTR_VALUE: return "META_DATA_ATTR_VALUE";
        case COL_META_DATA_ATTR_UNITS: return "META_DATA_ATTR_UNITS";
        default: return "";
    }
}

irods::error db_gen_query_op(irods::plugin_context& _ctx, genQueryInp_t* _inp, genQueryOut_t* _out) {
    rodsLog(LOG_NOTICE, "L3_PLUGIN: db_gen_query_op called");
    if (!_inp || !_out) return ERROR(SYS_INVALID_INPUT_PARAM, "null genQueryInp_t/Out_t");

    namespace gq = irods::experimental::genquery2;
    gq::select ast;

    // Map Projections
    for (int i = 0; i < _inp->selectInp.len; ++i) {
        int inx = _inp->selectInp.inx[i];
        std::string name = get_column_name_v4(inx);
        if (!name.empty()) {
            rodsLog(LOG_NOTICE, "L3_PLUGIN: Mapping projection [%d] -> [%s]", inx, name.c_str());
            ast.projections.push_back(gq::column{name});
        } else {
            rodsLog(LOG_NOTICE, "L3_PLUGIN: Unmapped projection index [%d], using typed fallback", inx);
            // Map to a column that is on the SAME node type to avoid traversal errors
            if (inx >= 100 && inx < 200) ast.projections.push_back(gq::column{"ZONE_NAME"});
            else if (inx >= 200 && inx < 300) ast.projections.push_back(gq::column{"USER_NAME"});
            else if (inx >= 300 && inx < 400) ast.projections.push_back(gq::column{"RESC_NAME"});
            else if (inx >= 400 && inx < 500) ast.projections.push_back(gq::column{"DATA_NAME"});
            else if (inx >= 500 && inx < 600) ast.projections.push_back(gq::column{"COLL_NAME"});
            else ast.projections.push_back(gq::column{"DATA_NAME"});
        }
    }

    // Map Conditions
    for (int i = 0; i < _inp->sqlCondInp.len; ++i) {
        int inx = _inp->sqlCondInp.inx[i];
        std::string val = _inp->sqlCondInp.value[i];
        std::string name = get_column_name_v4(inx);
        
        if (!name.empty()) {
            rodsLog(LOG_NOTICE, "L3_PLUGIN: Mapping condition [%d] (%s) -> [%s]", inx, name.c_str(), val.c_str());
            gq::column col{name};
            
            if (val.find("parent_of") != std::string::npos) {
                 std::string path = val;
                 auto start = path.find('\'');
                 auto end = path.find_last_of('\'');
                 if (start != std::string::npos && end != std::string::npos && end > start) {
                     path = path.substr(start + 1, end - start - 1);
                 }
                 auto pos = path.find_last_of('/');
                 std::string parent = (pos == 0) ? "/" : (pos == std::string::npos ? path : path.substr(0, pos));
                 rodsLog(LOG_NOTICE, "L3_PLUGIN: parent_of [%s] -> [%s]", path.c_str(), parent.c_str());
                 ast.conditions.push_back(gq::condition{col, gq::condition_equal{parent}});
            } else if (val.find("like") != std::string::npos) {
                 std::string pattern = val;
                 auto start = pattern.find('\'');
                 auto end = pattern.find_last_of('\'');
                 if (start != std::string::npos && end != std::string::npos && end > start) {
                     pattern = pattern.substr(start + 1, end - start - 1);
                 }
                 if (pattern == "_%") {
                     rodsLog(LOG_NOTICE, "L3_PLUGIN: ignoring like _%% condition for prototype");
                 } else {
                     rodsLog(LOG_NOTICE, "L3_PLUGIN: like pattern [%s]", pattern.c_str());
                     ast.conditions.push_back(gq::condition{col, gq::condition_equal{pattern}});
                 }
            } else if (val.find("=") != std::string::npos) {
                std::string pure_val = val.substr(val.find("=") + 1);
                pure_val.erase(0, pure_val.find_first_not_of(" \t'\""));
                pure_val.erase(pure_val.find_last_not_of(" \t'\"") + 1);
                ast.conditions.push_back(gq::condition{col, gq::condition_equal{pure_val}});
            } else {
                ast.conditions.push_back(gq::condition{col, gq::condition_equal{val}});
            }
        }
    }

    irods::catalog::ResultSet results;
    auto ret = g_catalog->execute_query(ast, results);
    if (!ret.ok()) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: execute_query failed: %s", ret.result().c_str());
        _out->rowCnt = 0;
        _out->attriCnt = static_cast<int>(_inp->selectInp.len);
        return SUCCESS();
    }

    _out->rowCnt = static_cast<int>(results.row_count());
    _out->attriCnt = static_cast<int>(_inp->selectInp.len);
    rodsLog(LOG_NOTICE, "L3_PLUGIN: Query produced [%d] rows", _out->rowCnt);
    
    if (_out->rowCnt == 0) {
        return ERROR(CAT_NO_ROWS_FOUND, "No rows found");
    }

    for (int j = 0; j < _out->attriCnt; ++j) {
        _out->sqlResult[j].attriInx = _inp->selectInp.inx[j];
        _out->sqlResult[j].len = 1024; 
        if (_out->rowCnt > 0) {
            _out->sqlResult[j].value = (char*)malloc(_out->rowCnt * 1024);
            memset(_out->sqlResult[j].value, 0, _out->rowCnt * 1024);

            for (int i = 0; i < _out->rowCnt; ++i) {
                std::string f(results.get_field(i, (size_t)j)); 
                const char* out_val = f.c_str();
                size_t out_len = f.size();
                if (f.empty() && (_out->sqlResult[j].attriInx % 100 == 1 || _out->sqlResult[j].attriInx == 500)) {
                    out_val = "0";
                    out_len = 1;
                }
                strncpy(_out->sqlResult[j].value + (i * 1024), out_val, std::min((size_t)1023, out_len));
            }
        } else {
            _out->sqlResult[j].value = nullptr;
        }
    }

    return SUCCESS();
}

irods::error db_check_auth_op(irods::plugin_context& _ctx, const char* _scheme, const char* _challenge, const char* _response, const char* _user_name, int* _user_priv_level, int* _client_priv_level) {
    rodsLog(LOG_NOTICE, "L3_PLUGIN: db_check_auth_op called for user [%s], scheme [%s]", _user_name, _scheme);
    if (!_user_name || !_user_priv_level || !_client_priv_level) return ERROR(SYS_INVALID_INPUT_PARAM, "null check_auth params");
    
    std::string user_str(_user_name);
    std::string user_name = user_str;
    std::string zone_name = "";
    
    auto pos = user_str.find('#');
    if (pos != std::string::npos) {
        user_name = user_str.substr(0, pos);
        zone_name = user_str.substr(pos + 1);
    } else {
        const auto& config_handle{irods::server_properties::instance().map()};
        zone_name = config_handle.get_json().at(KW_CFG_ZONE_NAME).get<std::string>();
    }

    rodsLog(LOG_NOTICE, "L3_PLUGIN: check_auth parsed user [%s], zone [%s]", user_name.c_str(), zone_name.c_str());
    
    auto ret = g_catalog->check_auth(user_name, zone_name, *_user_priv_level);
    if (ret.ok()) {
        rodsLog(LOG_NOTICE, "L3_PLUGIN: check_auth success for [%s#%s], priv level [%d]", user_name.c_str(), zone_name.c_str(), *_user_priv_level);
        *_client_priv_level = *_user_priv_level;
    } else {
        rodsLog(LOG_ERROR, "L3_PLUGIN: check_auth failed for [%s#%s]: %s", user_name.c_str(), zone_name.c_str(), ret.result().c_str());
    }
    return ret;
}

irods::error db_reg_zone_op(irods::plugin_context& _ctx, const char* _zone, const char* _type, const char* _conn) {
    rodsLog(LOG_NOTICE, "L3_PLUGIN: db_reg_zone_op called for [%s]", _zone);
    if (!_zone) return ERROR(SYS_INVALID_INPUT_PARAM, "null zone name");
    return g_catalog->register_zone({_zone, _type ? _type : "", _conn ? _conn : ""});
}

irods::error db_mod_zone_op(irods::plugin_context& _ctx, const char* _zone, const char* _prop, const char* _val) {
    rodsLog(LOG_NOTICE, "L3_PLUGIN: db_mod_zone_op called for [%s]", _zone);
    if (!_zone || !_prop || !_val) return ERROR(SYS_INVALID_INPUT_PARAM, "null zone mod params");
    return g_catalog->modify_zone(_zone, _prop, _val);
}

irods::error db_del_zone_op(irods::plugin_context& _ctx, const char* _zone) {
    rodsLog(LOG_NOTICE, "L3_PLUGIN: db_del_zone_op called for [%s]", _zone);
    if (!_zone) return ERROR(SYS_INVALID_INPUT_PARAM, "null zone name");
    return g_catalog->delete_zone(_zone);
}

irods::error db_reg_resc_op(irods::plugin_context& _ctx, std::map<std::string, std::string>* _info) {
    if (!_info) return ERROR(SYS_INVALID_INPUT_PARAM, "null resc map");
    rodsLog(LOG_NOTICE, "L3_PLUGIN: db_reg_resc_op called for [%s]", (*_info)["resc_name"].c_str());
    
    irods::catalog::resource resc;
    try {
        resc.id = std::stoll((*_info)["resc_id"]);
        resc.name = (*_info)["resc_name"];
        resc.type = (*_info)["resc_type"];
        resc.location = (*_info)["resc_net"];
        resc.vault_path = (*_info)["resc_def_path"];
        resc.context = (*_info)["resc_context"];
    } catch(...) { return ERROR(-1, "invalid resc map values"); }

    irods::catalog::resc_id_t out_id;
    return g_catalog->register_resource(resc, out_id);
}

irods::error db_mod_resc_op(irods::plugin_context& _ctx, const char* _resc, const char* _prop, const char* _val) {
    rodsLog(LOG_NOTICE, "L3_PLUGIN: db_mod_resc_op called for [%s]", _resc);
    if (!_resc) return ERROR(SYS_INVALID_INPUT_PARAM, "null resc name");
    irods::catalog::resc_id_t rid;
    if (auto ret = g_catalog->resolve_resource_name(_resc, rid); !ret.ok()) return ret;
    return g_catalog->modify_resource(rid, _prop, _val);
}

irods::error db_del_resc_op(irods::plugin_context& _ctx, const char* _resc) {
    rodsLog(LOG_NOTICE, "L3_PLUGIN: db_del_resc_op called for [%s]", _resc);
    if (!_resc) return ERROR(SYS_INVALID_INPUT_PARAM, "null resc name");
    irods::catalog::resc_id_t rid;
    if (auto ret = g_catalog->resolve_resource_name(_resc, rid); !ret.ok()) return ret;
    return g_catalog->delete_resource(rid);
}

irods::error db_mod_user_op(irods::plugin_context& _ctx, const char* _user, const char* _option, const char* _value) {
    rodsLog(LOG_NOTICE, "L3_PLUGIN: db_mod_user_op called for [%s]", _user);
    return SUCCESS();
}

irods::error db_reg_coll_op(irods::plugin_context& _ctx, collInfo_t* _info) {
    if (!_info) return ERROR(SYS_INVALID_INPUT_PARAM, "null collInfo_t");
    rodsLog(LOG_NOTICE, "L3_PLUGIN: db_reg_coll_op called for [%s]", _info->collName);
    irods::catalog::collection coll;
    coll.id = _info->collId;
    coll.name = _info->collName;
    coll.owner_name = _info->collOwnerName;
    coll.owner_zone = _info->collOwnerZone;

    if (coll.id <= 0) {
        if (auto ret = g_catalog->get_next_sequence_value("R_COLL_MAIN", coll.id); !ret.ok()) {
            return ret;
        }
    }

    irods::catalog::coll_id_t out_id;
    auto ret = g_catalog->register_collection(coll, out_id);
    if (ret.ok()) _info->collId = out_id;
    return ret;
}

irods::error db_reg_rule_exec_op(irods::plugin_context& _ctx, ruleExecSubmitInp_t* _inp) { return SUCCESS(); }
irods::error db_mod_rule_exec_op(irods::plugin_context& _ctx, const char* _re_id, keyValPair_t* _reg_param) { return SUCCESS(); }
irods::error db_del_rule_exec_op(irods::plugin_context& _ctx, const char* _re_id) { return SUCCESS(); }

irods::error db_delay_rule_lock_op(irods::plugin_context& _ctx, const char* _re_id, const char* _lock_id) {
    return SUCCESS();
}

irods::error db_delay_rule_unlock_op(irods::plugin_context& _ctx, const char* _re_id, const char* _lock_id) {
    return SUCCESS();
}

irods::error db_mod_group_op(irods::plugin_context& _ctx, const char* _group, const char* _option, const char* _user, const char* _zone) {
    rodsLog(LOG_NOTICE, "L3_PLUGIN: db_mod_group_op called for [%s]", _group);
    return SUCCESS();
}

irods::error db_rename_coll_op(irods::plugin_context& _ctx, const char* _old_name, const char* _new_name) {
    rodsLog(LOG_NOTICE, "L3_PLUGIN: db_rename_coll_op called for [%s] to [%s]", _old_name, _new_name);
    return g_catalog->rename_collection(_old_name, _new_name);
}

irods::error db_del_coll_op(irods::plugin_context& _ctx, collInfo_t* _info) {
    rodsLog(LOG_NOTICE, "L3_PLUGIN: db_del_coll_op called for [%s]", _info->collName);
    return g_catalog->delete_collection(_info->collId);
}

irods::error db_rename_object_op(irods::plugin_context& _ctx, rodsLong_t _obj_id, const char* _new_name) {
    rodsLog(LOG_NOTICE, "L3_PLUGIN: db_rename_object_op called for [%lld] to [%s]", _obj_id, _new_name);
    return g_catalog->rename_data_object(_obj_id, _new_name);
}

irods::error db_move_object_op(irods::plugin_context& _ctx, rodsLong_t _obj_id, rodsLong_t _target_coll_id) {
    rodsLog(LOG_NOTICE, "L3_PLUGIN: db_move_object_op called for [%lld] to coll [%lld]", _obj_id, _target_coll_id);
    return g_catalog->move_data_object(_obj_id, _target_coll_id);
}

irods::error db_make_session_token_op(irods::plugin_context& _ctx, char* _token, int _len) { return SUCCESS(); }
irods::error db_check_session_token_op(irods::plugin_context& _ctx, const char* _token) { return SUCCESS(); }

irods::error db_add_avu_metadata_op(irods::plugin_context& _ctx, int _wild, const char* _type, const char* _target_id, const char* _attr, const char* _val, const char* _units) {
    rodsLog(LOG_NOTICE, "L3_PLUGIN: db_add_avu_metadata_op called for [%s]:[%s]", _type, _target_id);
    return g_catalog->add_avu_metadata(_type, _target_id, {_attr, _val, _units ? _units : ""});
}

irods::error db_del_avu_metadata_op(irods::plugin_context& _ctx, int _wild, const char* _type, const char* _target_id, const char* _attr, const char* _val, const char* _units, int _unused) {
    rodsLog(LOG_NOTICE, "L3_PLUGIN: db_del_avu_metadata_op called for [%s]:[%s]", _type, _target_id);
    return g_catalog->delete_avu_metadata(_type, _target_id, {_attr, _val, _units ? _units : ""});
}

irods::error db_mod_avu_metadata_op(irods::plugin_context& _ctx, const char* _type, const char* _target_id, const char* _old_attr, const char* _old_val, const char* _old_units, const char* _new_attr, const char* _new_val, const char* _new_units, const char* _u1, int _u2) {
    rodsLog(LOG_NOTICE, "L3_PLUGIN: db_mod_avu_metadata_op called for [%s]:[%s]", _type, _target_id);
    return g_catalog->modify_avu_metadata(_type, _target_id, {_old_attr, _old_val, _old_units ? _old_units : ""}, {_new_attr, _new_val, _new_units ? _new_units : ""});
}

irods::error db_copy_avu_metadata_op(irods::plugin_context& _ctx, const char* _src_type, const char* _src_id, const char* _dst_type, const char* _dst_id) {
    rodsLog(LOG_NOTICE, "L3_PLUGIN: db_copy_avu_metadata_op called");
    return g_catalog->copy_avu_metadata(_src_type, _src_id, _dst_type, _dst_id);
}

irods::error db_set_avu_metadata_op(irods::plugin_context& _ctx, const char* _type, const char* _target_id, const char* _attr, const char* _val, const char* _units) {
    rodsLog(LOG_NOTICE, "L3_PLUGIN: db_set_avu_metadata_op called");
    return g_catalog->set_avu_metadata(_type, _target_id, {_attr, _val, _units ? _units : ""});
}

irods::error db_mod_access_control_op(irods::plugin_context& _ctx, int _recursive, const char* _access_level, const char* _user, const char* _zone, const char* _path) {
    rodsLog(LOG_NOTICE, "L3_PLUGIN: db_mod_access_control_op called");
    return SUCCESS();
}

irods::error db_execute_genquery2_sql(irods::plugin_context& _ctx, const char* _sql, const std::vector<std::string>* _bind_values, char** _output) {
    rodsLog(LOG_NOTICE, "L3_PLUGIN: db_execute_genquery2_sql called");
    if (!_sql || !_output) return ERROR(SYS_INVALID_INPUT_PARAM, "null sql or output");

    irods::experimental::genquery2::driver driver;
    if (const auto ec = driver.parse(_sql); ec != 0) {
        rodsLog(LOG_ERROR, "L3KVG: Failed to parse GenQuery2 string: %s", _sql);
        return ERROR(ec, "Failed to parse GenQuery2 string");
    }

    irods::catalog::ResultSet results;
    auto ret = g_catalog->execute_query(driver.select, results);
    if (!ret.ok()) return ret;

    *_output = strdup("[]"); 
    return SUCCESS();
}

irods::error db_check_auth_credentials_op(irods::plugin_context& _ctx, const char* _username, const char* _zone, const char* _password, int* _correct) {
    rodsLog(LOG_NOTICE, "L3_PLUGIN: db_check_auth_credentials_op called for user [%s#%s]", _username, _zone);
    if (!_username || !_zone || !_password || !_correct) return ERROR(SYS_INVALID_INPUT_PARAM, "null credentials params");
    
    *_correct = 0; 
    return SUCCESS();
}

class l3kvg_database_plugin : public irods::database {
public:
    l3kvg_database_plugin(const std::string& _inst, const std::string& _ctx)
        : irods::database(_inst, _ctx) {
        
        set_start_operation(db_start_operation);

        add_operation(irods::DATABASE_OP_START, std::function<irods::error(irods::plugin_context&)>(db_start_op));
        add_operation(irods::DATABASE_OP_OPEN, std::function<irods::error(irods::plugin_context&)>(db_open_op));
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

        add_operation<const char*, const char*, const char*, const char*, int*, int*>(
            irods::DATABASE_OP_CHECK_AUTH,
            std::function<irods::error(irods::plugin_context&, const char*, const char*, const char*, const char*, int*, int*)>(db_check_auth_op));

        add_operation<const char*, const char*, const char*, int*>(
            irods::DATABASE_OP_CHECK_AUTH_CREDENTIALS,
            std::function<irods::error(irods::plugin_context&, const char*, const char*, const char*, int*)>(db_check_auth_credentials_op));

        add_operation<const char*, const char*, const char*>(
            irods::DATABASE_OP_MOD_USER,
            std::function<irods::error(irods::plugin_context&, const char*, const char*, const char*)>(db_mod_user_op));

        add_operation<const char*, const char*, const char*, const char*>(
            irods::DATABASE_OP_MOD_GROUP,
            std::function<irods::error(irods::plugin_context&, const char*, const char*, const char*, const char*)>(db_mod_group_op));

        add_operation<const char*, const char*>(
            irods::DATABASE_OP_RENAME_COLL,
            std::function<irods::error(irods::plugin_context&, const char*, const char*)>(db_rename_coll_op));

        add_operation<collInfo_t*>(
            irods::DATABASE_OP_DEL_COLL,
            std::function<irods::error(irods::plugin_context&, collInfo_t*)>(db_del_coll_op));

        add_operation<rodsLong_t, const char*>(
            irods::DATABASE_OP_RENAME_OBJECT,
            std::function<irods::error(irods::plugin_context&, rodsLong_t, const char*)>(db_rename_object_op));

        add_operation<rodsLong_t, rodsLong_t>(
            irods::DATABASE_OP_MOVE_OBJECT,
            std::function<irods::error(irods::plugin_context&, rodsLong_t, rodsLong_t)>(db_move_object_op));

        add_operation<ruleExecSubmitInp_t*>(
            irods::DATABASE_OP_REG_RULE_EXEC,
            std::function<irods::error(irods::plugin_context&, ruleExecSubmitInp_t*)>(db_reg_rule_exec_op));

        add_operation<const char*, keyValPair_t*>(
            irods::DATABASE_OP_MOD_RULE_EXEC,
            std::function<irods::error(irods::plugin_context&, const char*, keyValPair_t*)>(db_mod_rule_exec_op));

        add_operation<const char*>(
            irods::DATABASE_OP_DEL_RULE_EXEC,
            std::function<irods::error(irods::plugin_context&, const char*)>(db_del_rule_exec_op));

        add_operation<const char*, const char*>(
            irods::DATABASE_OP_DELAY_RULE_LOCK,
            std::function<irods::error(irods::plugin_context&, const char*, const char*)>(db_delay_rule_lock_op));

        add_operation<const char*, const char*>(
            irods::DATABASE_OP_DELAY_RULE_UNLOCK,
            std::function<irods::error(irods::plugin_context&, const char*, const char*)>(db_delay_rule_unlock_op));

        add_operation<collInfo_t*>(
            irods::DATABASE_OP_REG_COLL,
            std::function<irods::error(irods::plugin_context&, collInfo_t*)>(db_reg_coll_op));

        add_operation<char*, int>(
            irods::DATABASE_OP_MAKE_SESSION_TOKEN,
            std::function<irods::error(irods::plugin_context&, char*, int)>(db_make_session_token_op));

        add_operation<const char*>(
            irods::DATABASE_OP_CHECK_SESSION_TOKEN,
            std::function<irods::error(irods::plugin_context&, const char*)>(db_check_session_token_op));

        add_operation<int, const char*, const char*, const char*, const char*, const char*>(
            irods::DATABASE_OP_ADD_AVU_METADATA,
            std::function<irods::error(irods::plugin_context&, int, const char*, const char*, const char*, const char*, const char*)>(db_add_avu_metadata_op));

        add_operation<int, const char*, const char*, const char*, const char*, const char*, int>(
            irods::DATABASE_OP_DEL_AVU_METADATA,
            std::function<irods::error(irods::plugin_context&, int, const char*, const char*, const char*, const char*, const char*, int)>(db_del_avu_metadata_op));

        add_operation<const char*, const char*, const char*, const char*, const char*, const char*, const char*, const char*, const char*, int>(
            irods::DATABASE_OP_MOD_AVU_METADATA,
            std::function<irods::error(irods::plugin_context&, const char*, const char*, const char*, const char*, const char*, const char*, const char*, const char*, const char*, int)>(db_mod_avu_metadata_op));

        add_operation<const char*, const char*, const char*, const char*>(
            irods::DATABASE_OP_COPY_AVU_METADATA,
            std::function<irods::error(irods::plugin_context&, const char*, const char*, const char*, const char*)>(db_copy_avu_metadata_op));

        add_operation<const char*, const char*, const char*, const char*, const char*>(
            irods::DATABASE_OP_SET_AVU_METADATA,
            std::function<irods::error(irods::plugin_context&, const char*, const char*, const char*, const char*, const char*)>(db_set_avu_metadata_op));

        add_operation<const char*, const std::vector<std::string>*, char**>(
            irods::DATABASE_OP_EXECUTE_GENQUERY2_SQL,
            std::function<irods::error(irods::plugin_context&, const char*, const std::vector<std::string>*, char**)>(db_execute_genquery2_sql));

        add_operation<const char*, const char*, const char*>(
            irods::DATABASE_OP_REG_ZONE,
            std::function<irods::error(irods::plugin_context&, const char*, const char*, const char*)>(db_reg_zone_op));

        add_operation<const char*, const char*, const char*>(
            irods::DATABASE_OP_MOD_ZONE,
            std::function<irods::error(irods::plugin_context&, const char*, const char*, const char*)>(db_mod_zone_op));

        add_operation<const char*>(
            irods::DATABASE_OP_DEL_ZONE,
            std::function<irods::error(irods::plugin_context&, const char*)>(db_del_zone_op));

        add_operation<std::map<std::string, std::string>*>(
            irods::DATABASE_OP_REG_RESC,
            std::function<irods::error(irods::plugin_context&, std::map<std::string, std::string>*)>(db_reg_resc_op));

        add_operation<const char*, const char*, const char*>(
            irods::DATABASE_OP_MOD_RESC,
            std::function<irods::error(irods::plugin_context&, const char*, const char*, const char*)>(db_mod_resc_op));

        add_operation<const char*>(
            irods::DATABASE_OP_DEL_RESC,
            std::function<irods::error(irods::plugin_context&, const char*)>(db_del_resc_op));
    }
};

extern "C" irods::database* plugin_factory(const std::string& _inst_name, const std::string& _context) {
    return new l3kvg_database_plugin(_inst_name, _context);
}
