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

#ifndef KW_CFG_ZONE_NAME
#define KW_CFG_ZONE_NAME "zone_name"
#endif
#ifndef KW_CFG_ZONE_USER
#define KW_CFG_ZONE_USER "zone_user"
#endif

static std::unique_ptr<irods::catalog::CatalogFacade> g_catalog;

irods::error init_l3kvg_catalog() {
    if (g_catalog) return SUCCESS();
    try {
        irods::server_properties::instance().capture();
        const auto& config_handle{irods::server_properties::instance().map()};
        const auto& config_json{config_handle.get_json()};
        
        if (!config_json.contains("plugin_configuration") || !config_json.at("plugin_configuration").contains("database")) {
            return ERROR(SYS_CONFIG_FILE_ERR, "Missing plugin_configuration/database");
        }
        const auto& db_config = config_json.at("plugin_configuration").at("database");
        
        nlohmann::json spec_config;
        if (db_config.contains(irods::KW_CFG_PLUGIN_SPECIFIC_CONFIGURATION)) {
            spec_config = db_config.at(irods::KW_CFG_PLUGIN_SPECIFIC_CONFIGURATION);
        } else if (db_config.contains("l3kvg") && db_config.at("l3kvg").contains(irods::KW_CFG_PLUGIN_SPECIFIC_CONFIGURATION)) {
            spec_config = db_config.at("l3kvg").at(irods::KW_CFG_PLUGIN_SPECIFIC_CONFIGURATION);
        }

        irods::catalog::Config cfg;
        cfg.db_path = spec_config.at("db_path").get<std::string>();
        cfg.node_id = spec_config.at("node_id").get<uint32_t>();
        cfg.zmq_endpoint = spec_config.contains("zmq_endpoint") ? spec_config.at("zmq_endpoint").get<std::string>() : "tcp://127.0.0.1:5555";
        rodsLog(LOG_NOTICE, "L3_PLUGIN: Using ZMQ endpoint [%s]", cfg.zmq_endpoint.c_str());

        if (spec_config.contains("federation")) {
            for (const auto& fed : spec_config.at("federation")) {
                cfg.federation.push_back({fed.at("name").get<std::string>(), fed.at("id").get<uint16_t>(), fed.at("endpoint").get<std::string>()});
            }
        }

        g_catalog = std::make_unique<irods::catalog::CatalogFacade>();
        if (auto ret = g_catalog->init(cfg); !ret.ok()) return ret;

        const std::string& zone_name = config_json.at(KW_CFG_ZONE_NAME).get<std::string>();
        const std::string& admin_name = config_json.at(KW_CFG_ZONE_USER).get<std::string>();
        g_catalog->bootstrap_catalog(zone_name, admin_name);
        if (!cfg.federation.empty()) g_catalog->bootstrap_federation(cfg.federation);

        return SUCCESS();
    } catch (const std::exception& e) {
        return ERROR(SYS_CONFIG_FILE_ERR, e.what());
    }
}

irods::error db_maintenance_op(irods::lookup_table<boost::any>& _props) { return init_l3kvg_catalog(); }
irods::error db_start_op(irods::plugin_context& _ctx) { return init_l3kvg_catalog(); }
irods::error db_stop_op(irods::plugin_context& _ctx) { g_catalog.reset(); return SUCCESS(); }
irods::error db_open_op(irods::plugin_context& _ctx) { return SUCCESS(); }
irods::error db_close_op(irods::plugin_context& _ctx) { return SUCCESS(); }

// Data Objects
irods::error db_reg_data_obj_op(irods::plugin_context& _ctx, dataObjInfo_t* _info) {
    if (!_info) return ERROR(SYS_INVALID_INPUT_PARAM, "null dataObjInfo_t");
    irods::catalog::data_object obj;
    obj.id = (uint64_t)_info->dataId; obj.coll_id = (uint64_t)_info->collId; obj.name = _info->objPath; obj.size = (uint64_t)_info->dataSize;
    obj.owner_name = _info->dataOwnerName; obj.owner_zone = _info->dataOwnerZone;
    obj.create_ts = _info->dataCreate; obj.modify_ts = _info->dataModify;
    obj.checksum = _info->chksum; obj.status = _info->statusString;
    if (obj.id <= 0) g_catalog->get_next_sequence_value("R_DATA_MAIN", obj.id);
    irods::catalog::data_id_t out_id;
    auto ret = g_catalog->register_data_object(obj, out_id);
    if (ret.ok()) _info->dataId = out_id;
    return ret;
}

irods::error db_mod_data_obj_meta_op(irods::plugin_context& _ctx, dataObjInfo_t* _info, keyValPair_t* _reg_param) {
    return g_catalog->modify_data_object((uint64_t)_info->dataId, "", "");
}

irods::error db_rename_object_op(irods::plugin_context& _ctx, rodsLong_t _obj_id, const char* _new_name) {
    return g_catalog->rename_data_object((uint64_t)_obj_id, _new_name);
}

irods::error db_move_object_op(irods::plugin_context& _ctx, rodsLong_t _obj_id, rodsLong_t _target_coll_id) {
    return g_catalog->move_data_object((uint64_t)_obj_id, (uint64_t)_target_coll_id);
}

// Replicas
irods::error db_reg_replica_op(irods::plugin_context& _ctx, dataObjInfo_t* _src, dataObjInfo_t* _dst, keyValPair_t* _cond) {
    irods::catalog::replica repl{(uint64_t)_dst->dataId, (uint32_t)_dst->replNum, (uint64_t)_dst->rescId, _dst->filePath, _dst->rescHier, _dst->statusString, _dst->chksum, _dst->dataModify, ""};
    return g_catalog->register_replica(repl);
}

irods::error db_unreg_replica_op(irods::plugin_context& _ctx, dataObjInfo_t* _info, keyValPair_t* _cond) {
    return g_catalog->unregister_replica((uint64_t)_info->dataId, (uint32_t)_info->replNum);
}

irods::error db_update_replica_access_time(irods::plugin_context& _ctx, const char* _data_id, char** _out) {
    return g_catalog->update_replica_access_time(std::stoull(_data_id), 0, "now");
}

// Collections
irods::error db_reg_coll_op(irods::plugin_context& _ctx, collInfo_t* _info) {
    irods::catalog::collection coll;
    coll.id = (uint64_t)_info->collId; coll.name = _info->collName; coll.owner_name = _info->collOwnerName; coll.owner_zone = _info->collOwnerZone;
    if (coll.id <= 0) g_catalog->get_next_sequence_value("R_COLL_MAIN", coll.id);
    irods::catalog::coll_id_t out_id;
    auto ret = g_catalog->register_collection(coll, out_id);
    if (ret.ok()) _info->collId = out_id;
    return ret;
}

irods::error db_mod_coll_op(irods::plugin_context& _ctx, collInfo_t* _info) {
    return g_catalog->modify_collection((uint64_t)_info->collId, "", "");
}

irods::error db_del_coll_op(irods::plugin_context& _ctx, collInfo_t* _info) {
    return g_catalog->delete_collection((uint64_t)_info->collId);
}

irods::error db_rename_coll_op(irods::plugin_context& _ctx, const char* _old_name, const char* _new_name) {
    return g_catalog->rename_collection(_old_name, _new_name);
}

// Resources
irods::error db_reg_resc_op(irods::plugin_context& _ctx, std::map<std::string, std::string>* _info) {
    irods::catalog::resource resc;
    resc.id = std::stoll((*_info)["resc_id"]); resc.name = (*_info)["resc_name"]; resc.type = (*_info)["resc_type"];
    resc.location = (*_info)["resc_net"]; resc.vault_path = (*_info)["resc_def_path"]; resc.context = (*_info)["resc_context"];
    irods::catalog::resc_id_t out_id;
    return g_catalog->register_resource(resc, out_id);
}

irods::error db_mod_resc_op(irods::plugin_context& _ctx, const char* _resc, const char* _prop, const char* _val) {
    return g_catalog->modify_resource(0, _prop, _val);
}

irods::error db_del_resc_op(irods::plugin_context& _ctx, const char* _resc, int _unused) {
    return SUCCESS();
}

// Identity
irods::error db_reg_user_re_op(irods::plugin_context& _ctx, userInfo_t* _info) {
    irods::catalog::user usr;
    usr.id = (uint64_t)_info->sysUid; usr.name = _info->userName; usr.zone = _info->rodsZone; usr.type = _info->userType;
    if (usr.id <= 0) g_catalog->get_next_sequence_value("R_USER_MAIN", usr.id);
    irods::catalog::user_id_t out_id;
    return g_catalog->register_user(usr, out_id);
}

irods::error db_mod_user_op(irods::plugin_context& _ctx, const char* _user, const char* _option, const char* _value) {
    return g_catalog->modify_user(0, _option, _value);
}

irods::error db_check_auth_op(irods::plugin_context& _ctx, const char* _scheme, const char* _challenge, const char* _response, const char* _user_name, int* _user_priv_level, int* _client_priv_level) {
    std::string user_str(_user_name);
    std::string user_name = user_str, zone_name = "";
    auto pos = user_str.find('#');
    if (pos != std::string::npos) { user_name = user_str.substr(0, pos); zone_name = user_str.substr(pos + 1); }
    else { zone_name = irods::server_properties::instance().map().get_json().at(KW_CFG_ZONE_NAME).get<std::string>(); }
    auto ret = g_catalog->check_auth(user_name, zone_name, *_user_priv_level);
    if (ret.ok()) *_client_priv_level = *_user_priv_level;
    return ret;
}

irods::error db_check_auth_credentials_op(irods::plugin_context& _ctx, const char* _username, const char* _zone, const char* _password, int* _correct) {
    bool correct = false;
    auto ret = g_catalog->check_auth_credentials(_username, _zone, _password, correct);
    *_correct = correct ? 1 : 0;
    return ret;
}

// Metadata
irods::error db_add_avu_metadata_op(irods::plugin_context& _ctx, const char* _type, const char* _target_id, const char* _attr, const char* _val, const char* _units, const KeyValPair* _unused) {
    return g_catalog->add_avu_metadata(_type, _target_id, {_attr, _val, _units ? _units : ""});
}

irods::error db_set_avu_metadata_op(irods::plugin_context& _ctx, const char* _type, const char* _target_id, const char* _attr, const char* _val, const char* _units, const KeyValPair* _unused) {
    return g_catalog->set_avu_metadata(_type, _target_id, {_attr, _val, _units ? _units : ""});
}

irods::error db_mod_access_control_op(irods::plugin_context& _ctx, int _recursive, const char* _access_level, const char* _user, const char* _zone, const char* _path) {
    return g_catalog->set_access(0, 0, _access_level, _recursive != 0);
}

// Zones
irods::error db_reg_zone_op(irods::plugin_context& _ctx, const char* _zone, const char* _type, const char* _conn, const char* _comment) {
    return g_catalog->register_zone({_zone, _type ? _type : "", _conn ? _conn : "", _comment ? _comment : ""});
}

irods::error db_mod_zone_op(irods::plugin_context& _ctx, const char* _zone, const char* _prop, const char* _val) {
    return g_catalog->modify_zone(_zone, _prop, _val);
}

irods::error db_del_zone_op(irods::plugin_context& _ctx, const char* _zone) {
    return g_catalog->delete_zone(_zone);
}

// GenQuery
irods::error db_gen_query_op(irods::plugin_context& _ctx, genQueryInp_t* _inp, genQueryOut_t* _out) {
    return ERROR(CAT_NO_ROWS_FOUND, "GenQuery not yet implemented via Smart Client");
}

class l3kvg_database_plugin : public irods::database {
public:
    l3kvg_database_plugin(const std::string& _inst, const std::string& _ctx) : irods::database(_inst, _ctx) {
        set_start_operation(db_maintenance_op);
        add_operation(irods::DATABASE_OP_START, std::function<irods::error(irods::plugin_context&)>(db_start_op));
        add_operation(irods::DATABASE_OP_STOP, std::function<irods::error(irods::plugin_context&)>(db_stop_op));
        add_operation(irods::DATABASE_OP_OPEN, std::function<irods::error(irods::plugin_context&)>(db_open_op));
        add_operation(irods::DATABASE_OP_CLOSE, std::function<irods::error(irods::plugin_context&)>(db_close_op));
        
        add_operation<dataObjInfo_t*>(irods::DATABASE_OP_REG_DATA_OBJ, std::function<irods::error(irods::plugin_context&, dataObjInfo_t*)>(db_reg_data_obj_op));
        add_operation<dataObjInfo_t*, keyValPair_t*>(irods::DATABASE_OP_MOD_DATA_OBJ_META, std::function<irods::error(irods::plugin_context&, dataObjInfo_t*, keyValPair_t*)>(db_mod_data_obj_meta_op));
        add_operation<rodsLong_t, const char*>(irods::DATABASE_OP_RENAME_OBJECT, std::function<irods::error(irods::plugin_context&, rodsLong_t, const char*)>(db_rename_object_op));
        add_operation<rodsLong_t, rodsLong_t>(irods::DATABASE_OP_MOVE_OBJECT, std::function<irods::error(irods::plugin_context&, rodsLong_t, rodsLong_t)>(db_move_object_op));

        add_operation<dataObjInfo_t*, dataObjInfo_t*, keyValPair_t*>(irods::DATABASE_OP_REG_REPLICA, std::function<irods::error(irods::plugin_context&, dataObjInfo_t*, dataObjInfo_t*, keyValPair_t*)>(db_reg_replica_op));
        add_operation<dataObjInfo_t*, keyValPair_t*>(irods::DATABASE_OP_UNREG_REPLICA, std::function<irods::error(irods::plugin_context&, dataObjInfo_t*, keyValPair_t*)>(db_unreg_replica_op));
        add_operation<const char*, char**>(irods::DATABASE_OP_UPDATE_REPLICA_ACCESS_TIME, std::function<irods::error(irods::plugin_context&, const char*, char**)>(db_update_replica_access_time));

        add_operation<collInfo_t*>(irods::DATABASE_OP_REG_COLL, std::function<irods::error(irods::plugin_context&, collInfo_t*)>(db_reg_coll_op));
        add_operation<collInfo_t*>(irods::DATABASE_OP_MOD_COLL, std::function<irods::error(irods::plugin_context&, collInfo_t*)>(db_mod_coll_op));
        add_operation<collInfo_t*>(irods::DATABASE_OP_DEL_COLL, std::function<irods::error(irods::plugin_context&, collInfo_t*)>(db_del_coll_op));
        add_operation<const char*, const char*>(irods::DATABASE_OP_RENAME_COLL, std::function<irods::error(irods::plugin_context&, const char*, const char*)>(db_rename_coll_op));

        add_operation<std::map<std::string, std::string>*>(irods::DATABASE_OP_REG_RESC, std::function<irods::error(irods::plugin_context&, std::map<std::string, std::string>*)>(db_reg_resc_op));
        add_operation<const char*, const char*, const char*>(irods::DATABASE_OP_MOD_RESC, std::function<irods::error(irods::plugin_context&, const char*, const char*, const char*)>(db_mod_resc_op));
        add_operation<const char*, int>(irods::DATABASE_OP_DEL_RESC, std::function<irods::error(irods::plugin_context&, const char*, int)>(db_del_resc_op));

        add_operation<userInfo_t*>(irods::DATABASE_OP_REG_USER_RE, std::function<irods::error(irods::plugin_context&, userInfo_t*)>(db_reg_user_re_op));
        add_operation<const char*, const char*, const char*>(irods::DATABASE_OP_MOD_USER, std::function<irods::error(irods::plugin_context&, const char*, const char*, const char*)>(db_mod_user_op));
        add_operation<const char*, const char*, const char*, const char*, int*, int*>(irods::DATABASE_OP_CHECK_AUTH, std::function<irods::error(irods::plugin_context&, const char*, const char*, const char*, const char*, int*, int*)>(db_check_auth_op));
        add_operation<const char*, const char*, const char*, int*>(irods::DATABASE_OP_CHECK_AUTH_CREDENTIALS, std::function<irods::error(irods::plugin_context&, const char*, const char*, const char*, int*)>(db_check_auth_credentials_op));

        add_operation<const char*, const char*, const char*, const char*, const char*, const KeyValPair*>(irods::DATABASE_OP_SET_AVU_METADATA, std::function<irods::error(irods::plugin_context&, const char*, const char*, const char*, const char*, const char*, const KeyValPair*)>(db_set_avu_metadata_op));
        add_operation<const char*, const char*, const char*, const char*, const char*, const KeyValPair*>(irods::DATABASE_OP_ADD_AVU_METADATA, std::function<irods::error(irods::plugin_context&, const char*, const char*, const char*, const char*, const char*, const KeyValPair*)>(db_add_avu_metadata_op));
        
        add_operation<const char*, const char*, const char*, const char*>(irods::DATABASE_OP_REG_ZONE, std::function<irods::error(irods::plugin_context&, const char*, const char*, const char*, const char*)>(db_reg_zone_op));
        add_operation<const char*, const char*, const char*>(irods::DATABASE_OP_MOD_ZONE, std::function<irods::error(irods::plugin_context&, const char*, const char*, const char*)>(db_mod_zone_op));
        add_operation<const char*>(irods::DATABASE_OP_DEL_ZONE, std::function<irods::error(irods::plugin_context&, const char*)>(db_del_zone_op));

        add_operation<int, const char*, const char*, const char*, const char*>(irods::DATABASE_OP_MOD_ACCESS_CONTROL, std::function<irods::error(irods::plugin_context&, int, const char*, const char*, const char*, const char*)>(db_mod_access_control_op));
        add_operation<genQueryInp_t*, genQueryOut_t*>(irods::DATABASE_OP_GEN_QUERY, std::function<irods::error(irods::plugin_context&, genQueryInp_t*, genQueryOut_t*)>(db_gen_query_op));
    }
};

extern "C" irods::database* plugin_factory(const std::string& _inst_name, const std::string& _context) { return new l3kvg_database_plugin(_inst_name, _context); }
