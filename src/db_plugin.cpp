#include "irods/irods_database_plugin.hpp"
#include "irods/irods_database_constants.hpp"
#include "irods/irods_server_properties.hpp"
#include "irods/irods_configuration_keywords.hpp"
#include "irods/catalog/catalog_facade.hpp"
#include "irods/filesystem/path.hpp"
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
#include <cstring>
#include <iostream>
#include <ctime>

inline std::string sanitize_utf8(std::string_view sv) {
    std::string res;
    res.reserve(sv.size());
    for (size_t i = 0; i < sv.size();) {
        unsigned char c = sv[i];
        if (c < 0x80) {
            res.push_back(c);
            i++;
        } else if ((c & 0xE0) == 0xC0) {
            if (i + 1 < sv.size() && (static_cast<unsigned char>(sv[i + 1]) & 0xC0) == 0x80) {
                res.push_back(sv[i]);
                res.push_back(sv[i + 1]);
                i += 2;
            } else {
                res.push_back('?');
                i++;
            }
        } else if ((c & 0xF0) == 0xE0) {
            if (i + 2 < sv.size() && (static_cast<unsigned char>(sv[i + 1]) & 0xC0) == 0x80 && (static_cast<unsigned char>(sv[i + 2]) & 0xC0) == 0x80) {
                res.push_back(sv[i]);
                res.push_back(sv[i + 1]);
                res.push_back(sv[i + 2]);
                i += 3;
            } else {
                res.push_back('?');
                i++;
            }
        } else if ((c & 0xF8) == 0xF0) {
            if (i + 3 < sv.size() && (static_cast<unsigned char>(sv[i + 1]) & 0xC0) == 0x80 && (static_cast<unsigned char>(sv[i + 2]) & 0xC0) == 0x80 && (static_cast<unsigned char>(sv[i + 3]) & 0xC0) == 0x80) {
                res.push_back(sv[i]);
                res.push_back(sv[i + 1]);
                res.push_back(sv[i + 2]);
                res.push_back(sv[i + 3]);
                i += 4;
            } else {
                res.push_back('?');
                i++;
            }
        } else {
            res.push_back('?');
            i++;
        }
    }
    return res;
}

inline std::string safe_string(const char* s) {
    if (!s) return "";
    return sanitize_utf8(s);
}

template <size_t N>
inline std::string safe_string(const char (&arr)[N]) {
    size_t len = 0;
    while (len < N && arr[len] != '\0') {
        len++;
    }
    return sanitize_utf8(std::string_view(arr, len));
}

static std::string get_timestamp(const std::string& ts) {
    if (ts.empty() || ts == "set time to now" || ts == "now") {
        return std::to_string(std::time(nullptr));
    }
    return ts;
}

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
        const auto& config_handle = irods::server_properties::instance().map();
        const auto& config_json = config_handle.get_json();
        
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
        cfg.db_path = spec_config.value("db_path", "/var/lib/irods/l3kvg_db");
        cfg.node_id = spec_config.value("node_id", (uint32_t)0);
        cfg.zmq_endpoint = spec_config.value("zmq_endpoint", "tcp://127.0.0.1:5555");

        if (spec_config.contains("federation")) {
            for (const auto& fed : spec_config.at("federation")) {
                cfg.federation.push_back({
                    fed.value("name", ""), 
                    (uint16_t)fed.value("id", 0), 
                    fed.value("endpoint", "")
                });
            }
        }

        const std::string& zone_name = config_json.value(KW_CFG_ZONE_NAME, "tempZone");
        cfg.cluster_id = irods::catalog::SnowflakeID::calculate_cluster_id(zone_name);

        l3kvg::Settings settings;
        settings.node_id = cfg.node_id;
        settings.fed_timeout_ms = 5000;

        g_catalog = std::make_unique<irods::catalog::CatalogFacade>();
        if (auto ret = g_catalog->init(cfg, zone_name, settings); !ret.ok()) {
            return ret;
        }

        return SUCCESS();
    } catch (const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: init_l3kvg_catalog EXCEPTION: %s", e.what());
        return ERROR(SYS_CONFIG_FILE_ERR, e.what());
    }
}

irods::error db_maintenance_op(irods::lookup_table<boost::any>& _props) { return init_l3kvg_catalog(); }
irods::error db_start_op(irods::plugin_context& _ctx) { return init_l3kvg_catalog(); }
irods::error db_stop_op(irods::plugin_context& _ctx) { g_catalog.reset(); return SUCCESS(); }
irods::error db_open_op(irods::plugin_context& _ctx) { return SUCCESS(); }
irods::error db_close_op(irods::plugin_context& _ctx) { return SUCCESS(); }
irods::error db_commit_op(irods::plugin_context& _ctx) { return SUCCESS(); }
irods::error db_rollback_op(irods::plugin_context& _ctx) { return SUCCESS(); }

// Data Objects
irods::error db_reg_data_obj_op(irods::plugin_context& _ctx, dataObjInfo_t* _info) {
    try {
        rodsLog(LOG_NOTICE, "L3_PLUGIN: ENTERING db_reg_data_obj_op");
        if (!_info) return ERROR(SYS_INVALID_INPUT_PARAM, "null dataObjInfo_t");
        irods::catalog::data_object obj;
        obj.id = (uint64_t)_info->dataId; 
        obj.coll_id = (uint64_t)_info->collId; 
        
        std::string full_path = safe_string(_info->objPath);
        obj.full_path = full_path;
        irods::experimental::filesystem::path p(full_path);
        obj.name = p.object_name().string();
        std::string parent_path = p.parent_path().string();

        if (obj.coll_id == 0) {
            irods::catalog::EntityType type;
            irods::catalog::snowflake_id_t sid;
            if (g_catalog->resolve_path(parent_path, sid, type).ok()) {
                // Fetch the node to get the sequential ID
                auto payload = g_catalog->get_client()->get_node_payload_async(g_catalog->get_cluster_id(), sid).get();
                if (!payload.empty()) {
                    lite3cpp::Buffer buf(std::vector<uint8_t>(payload.begin(), payload.end()));
                    obj.coll_id = buf.get_i64(0, "id");
                }
            }
        } 

        obj.size = (uint64_t)_info->dataSize;
        obj.owner_name = safe_string(_info->dataOwnerName); 
        if (obj.owner_name.empty()) {
            obj.owner_name = _ctx.comm()->clientUser.userName;
        }
        obj.owner_zone = safe_string(_info->dataOwnerZone);
        if (obj.owner_zone.empty()) {
            const auto& config = irods::server_properties::instance().map().get_json();
            obj.owner_zone = config.at(KW_CFG_ZONE_NAME).get<std::string>();
        }
        obj.create_ts = get_timestamp(safe_string(_info->dataCreate)); 
        obj.modify_ts = get_timestamp(safe_string(_info->dataModify));
        obj.type = safe_string(_info->dataType);
        if (obj.type.empty()) obj.type = "generic";
        obj.checksum = safe_string(_info->chksum);
        obj.status = safe_string(_info->statusString);

        if (obj.id <= 0) g_catalog->get_next_sequence_value("R_DATA_MAIN", obj.id);

        rodsLog(LOG_NOTICE, "L3_PLUGIN: db_reg_data_obj_op: obj.name=%s, obj.coll_id=%llu, obj.id=%llu", obj.name.c_str(), (unsigned long long)obj.coll_id, (unsigned long long)obj.id);

        irods::catalog::data_id_t out_id;
        auto ret = g_catalog->register_data_object(obj, out_id);
        if (ret.ok()) {
            _info->dataId = out_id;
            
            // Also register the initial replica, as R_DATA_MAIN traditionally holds both
            irods::catalog::replica repl;
            repl.data_id = (uint64_t)out_id;
            repl.replica_number = (uint32_t)_info->replNum;
            repl.resource_id = (uint64_t)_info->rescId;
            repl.physical_path = safe_string(_info->filePath);
            repl.resc_hier = safe_string(_info->rescHier);
            repl.status = std::to_string(_info->replStatus);
            repl.checksum = safe_string(_info->chksum);
            repl.modify_ts = get_timestamp(safe_string(_info->dataModify));

            if (repl.resource_id == 0 && _info->rescName[0] != '\0') {
                irods::catalog::snowflake_id_t rsid;
                if (g_catalog->resolve_resource_name(_info->rescName, rsid).ok()) {
                    auto payload = g_catalog->get_client()->get_node_payload_async(g_catalog->get_cluster_id(), rsid).get();
                    if (!payload.empty()) {
                        lite3cpp::Buffer buf(std::vector<uint8_t>(payload.begin(), payload.end()));
                        repl.resource_id = buf.get_i64(0, "id");
                    }
                }
            }
            
            auto repl_ret = g_catalog->register_replica(repl);
            if (!repl_ret.ok()) {
                rodsLog(LOG_ERROR, "L3_PLUGIN: db_reg_data_obj_op failed to register replica: %s", repl_ret.result().c_str());
            }
        }
        rodsLog(LOG_NOTICE, "L3_PLUGIN: EXITING db_reg_data_obj_op SUCCESS");
        return ret;
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_reg_data_obj_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

irods::error db_mod_data_obj_meta_op(irods::plugin_context& _ctx, dataObjInfo_t* _info, keyValPair_t* _reg_param) {
    try {
        rodsLog(LOG_NOTICE, "L3_PLUGIN: ENTERING db_mod_data_obj_meta_op");
        if (!_info) return ERROR(SYS_INVALID_INPUT_PARAM, "null dataObjInfo_t");
        if (_reg_param) {
            for (int i = 0; i < _reg_param->len; ++i) {
                g_catalog->modify_data_object((uint64_t)_info->dataId, 
                    safe_string(_reg_param->keyWord[i]), 
                    safe_string(_reg_param->value[i]));
            }
        }
        rodsLog(LOG_NOTICE, "L3_PLUGIN: EXITING db_mod_data_obj_meta_op SUCCESS");
        return SUCCESS();
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_mod_data_obj_meta_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

irods::error db_rename_object_op(irods::plugin_context& _ctx, rodsLong_t _obj_id, const char* _new_name) {
    try {
        rodsLog(LOG_NOTICE, "L3_PLUGIN: ENTERING db_rename_object_op");
        auto ret = g_catalog->rename_data_object((uint64_t)_obj_id, safe_string(_new_name));
        rodsLog(LOG_NOTICE, "L3_PLUGIN: EXITING db_rename_object_op SUCCESS");
        return ret;
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_rename_object_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

irods::error db_move_object_op(irods::plugin_context& _ctx, rodsLong_t _obj_id, rodsLong_t _target_coll_id) {
    try {
        rodsLog(LOG_NOTICE, "L3_PLUGIN: ENTERING db_move_object_op");
        auto ret = g_catalog->move_data_object((uint64_t)_obj_id, (uint64_t)_target_coll_id);
        rodsLog(LOG_NOTICE, "L3_PLUGIN: EXITING db_move_object_op SUCCESS");
        return ret;
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_move_object_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

// Replicas
irods::error db_reg_replica_op(irods::plugin_context& _ctx, dataObjInfo_t* _src, dataObjInfo_t* _dst, keyValPair_t* _cond) {
    try {
        rodsLog(LOG_NOTICE, "L3_PLUGIN: ENTERING db_reg_replica_op");
        if (!_dst) return ERROR(SYS_INVALID_INPUT_PARAM, "null dataObjInfo_t (dst)");
        irods::catalog::replica repl{
            (uint64_t)_dst->dataId, 
            (uint32_t)_dst->replNum, 
            (uint64_t)_dst->rescId, 
            safe_string(_dst->filePath), 
            safe_string(_dst->rescHier), 
            std::to_string(_dst->replStatus), 
            safe_string(_dst->chksum), 
            get_timestamp(safe_string(_dst->dataModify)), 
            ""};

        if (repl.resource_id == 0 && _dst->rescName[0] != '\0') {
            irods::catalog::snowflake_id_t rsid;
            if (g_catalog->resolve_resource_name(_dst->rescName, rsid).ok()) {
                auto payload = g_catalog->get_client()->get_node_payload_async(g_catalog->get_cluster_id(), rsid).get();
                if (!payload.empty()) {
                    lite3cpp::Buffer buf(std::vector<uint8_t>(payload.begin(), payload.end()));
                    repl.resource_id = buf.get_i64(0, "id");
                }
            }
        }
        
        auto ret = g_catalog->register_replica(repl);
        rodsLog(LOG_NOTICE, "L3_PLUGIN: EXITING db_reg_replica_op SUCCESS");
        return ret;
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_reg_replica_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

irods::error db_unreg_replica_op(irods::plugin_context& _ctx, dataObjInfo_t* _info, keyValPair_t* _cond) {
    try {
        rodsLog(LOG_NOTICE, "L3_PLUGIN: ENTERING db_unreg_replica_op");
        if (!_info) return ERROR(SYS_INVALID_INPUT_PARAM, "null dataObjInfo_t");
        auto ret = g_catalog->unregister_replica((uint64_t)_info->dataId, (uint32_t)_info->replNum);
        rodsLog(LOG_NOTICE, "L3_PLUGIN: EXITING db_unreg_replica_op SUCCESS");
        return ret;
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_unreg_replica_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

irods::error db_update_replica_access_time(irods::plugin_context& _ctx, const char* _json_input, char** _out) {
    try {
        rodsLog(LOG_NOTICE, "L3_PLUGIN: ENTERING db_update_replica_access_time");
        if (!_json_input || !_out) {
            return ERROR(SYS_INTERNAL_NULL_INPUT_ERR, "Received one or more null pointers.");
        }
        
        auto json_input = nlohmann::json::parse(_json_input);
        const auto& updates = json_input.at("access_time_updates");
        
        for (const auto& _j : updates) {
            uint64_t data_id = _j.at("data_id").get<uint64_t>();
            uint32_t repl_num = _j.at("replica_number").get<uint32_t>();
            std::string atime = _j.at("atime").get<std::string>();
            
            auto ret = g_catalog->update_replica_access_time(data_id, repl_num, atime);
            if (!ret.ok()) return ret;
        }
        
        rodsLog(LOG_NOTICE, "L3_PLUGIN: EXITING db_update_replica_access_time SUCCESS");
        return SUCCESS();
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_update_replica_access_time EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

// Collections
irods::error db_reg_coll_op(irods::plugin_context& _ctx, collInfo_t* _info) {
    try {
        rodsLog(LOG_NOTICE, "L3_PLUGIN: ENTERING db_reg_coll_op name [%s] owner [%s]", _info && _info->collName ? _info->collName : "null", _info && _info->collOwnerName ? _info->collOwnerName : "null");
        if (!_info) return ERROR(SYS_INVALID_INPUT_PARAM, "null collInfo_t");
        irods::catalog::collection coll;
        coll.id = (uint64_t)_info->collId; 
        coll.name = safe_string(_info->collName); 
        coll.parent_name = safe_string(_info->collParentName);
        coll.owner_name = safe_string(_info->collOwnerName);

        // Heuristic: If owner is empty, check if it's a home directory
        if (coll.owner_name.empty()) {
            std::string name = coll.name;
            size_t home_pos = name.find("/home/");
            if (home_pos != std::string::npos) {
                std::string sub = name.substr(home_pos + 6);
                size_t slash = sub.find('/');
                coll.owner_name = (slash == std::string::npos) ? sub : sub.substr(0, slash);
            }
        }

        if (coll.owner_name.empty()) {
            coll.owner_name = _ctx.comm()->clientUser.userName;
        }
        coll.owner_zone = safe_string(_info->collOwnerZone);
        if (coll.owner_zone.empty()) {
            coll.owner_zone = _ctx.comm()->clientUser.rodsZone;
        }

        if (coll.parent_id == 0 && !coll.parent_name.empty()) {
            irods::catalog::EntityType type;
            irods::catalog::snowflake_id_t sid;
            if (g_catalog->resolve_path(coll.parent_name, sid, type).ok()) {
                // Fetch the node to get the sequential ID
                auto payload = g_catalog->get_client()->get_node_payload_async(g_catalog->get_cluster_id(), sid).get();
                if (!payload.empty()) {
                    lite3cpp::Buffer buf(std::vector<uint8_t>(payload.begin(), payload.end()));
                    coll.parent_id = buf.get_i64(0, "id");
                }
            }
        }
        
        // Normal collections have empty type string, but some iRODS layers pass "collection"
        if (coll.type == "collection") coll.type = "";
        
        if (coll.owner_zone.empty()) {
            const auto& config = irods::server_properties::instance().map().get_json();
            coll.owner_zone = config.at(KW_CFG_ZONE_NAME).get<std::string>();
        }
        coll.create_ts = get_timestamp(safe_string(_info->collCreate));
        coll.modify_ts = get_timestamp(safe_string(_info->collModify));
        if (coll.id <= 0) g_catalog->get_next_sequence_value("R_COLL_MAIN", coll.id);
        irods::catalog::coll_id_t out_id;
        auto ret = g_catalog->register_collection(coll, out_id);
        if (ret.ok()) _info->collId = out_id;
        rodsLog(LOG_NOTICE, "L3_PLUGIN: EXITING db_reg_coll_op SUCCESS");
        return ret;
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_reg_coll_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

irods::error db_mod_coll_op(irods::plugin_context& _ctx, collInfo_t* _info) {
    try {
        rodsLog(LOG_NOTICE, "L3_PLUGIN: ENTERING db_mod_coll_op");
        if (!_info) return ERROR(SYS_INVALID_INPUT_PARAM, "null collInfo_t");
        for (int i = 0; i < _info->condInput.len; ++i) {
            g_catalog->modify_collection((uint64_t)_info->collId, 
                safe_string(_info->condInput.keyWord[i]), 
                safe_string(_info->condInput.value[i]));
        }
        rodsLog(LOG_NOTICE, "L3_PLUGIN: EXITING db_mod_coll_op SUCCESS");
        return SUCCESS();
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_mod_coll_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

irods::error db_del_coll_op(irods::plugin_context& _ctx, collInfo_t* _info) {
    try {
        rodsLog(LOG_NOTICE, "L3_PLUGIN: ENTERING db_del_coll_op");
        if (!_info) return ERROR(SYS_INVALID_INPUT_PARAM, "null collInfo_t");
        
        uint64_t coll_id = (uint64_t)_info->collId;
        if (coll_id == 0 && _info->collName && strlen(_info->collName) > 0) {
            irods::catalog::snowflake_id_t sid;
            irods::catalog::EntityType type;
            if (g_catalog->resolve_path(_info->collName, sid, type).ok()) {
                coll_id = sid;
            }
        }

        if (coll_id == 0) {
            rodsLog(LOG_ERROR, "L3_PLUGIN: db_del_coll_op: No collection ID or name provided");
            return ERROR(SYS_INVALID_INPUT_PARAM, "No collection ID or name provided");
        }

        auto ret = g_catalog->delete_collection(coll_id);
        rodsLog(LOG_NOTICE, "L3_PLUGIN: EXITING db_del_coll_op SUCCESS");
        return ret;
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_del_coll_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

irods::error db_rename_coll_op(irods::plugin_context& _ctx, const char* _old_name, const char* _new_name) {
    try {
        rodsLog(LOG_NOTICE, "L3_PLUGIN: ENTERING db_rename_coll_op");
        auto ret = g_catalog->rename_collection(safe_string(_old_name), safe_string(_new_name));
        rodsLog(LOG_NOTICE, "L3_PLUGIN: EXITING db_rename_coll_op SUCCESS");
        return ret;
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_rename_coll_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

// Resources
irods::error db_reg_resc_op(irods::plugin_context& _ctx, std::map<std::string, std::string>* _info) {
    try {
        rodsLog(LOG_NOTICE, "L3_PLUGIN: ENTERING db_reg_resc_op");
        if (auto ret = init_l3kvg_catalog(); !ret.ok()) return ret;
        irods::catalog::resource resc;
        if (_info) {
            for (auto const& [key, val] : *_info) {
                rodsLog(LOG_NOTICE, "L3_PLUGIN: db_reg_resc_op: key=[%s] val=[%s]", key.c_str(), val.c_str());
                if (key == "resource_property_id" || key == "resc_id" || key == "RESC_ID") resc.id = val.empty() ? 0 : std::stoull(val);
                else if (key == "resource_property_name" || key == "resc_name" || key == "RESC_NAME") resc.name = val;
                else if (key == "resource_property_type" || key == "resc_type_name" || key == "RESC_TYPE_NAME" || key == "resc_type") resc.type = val;
                else if (key == "resource_property_location" || key == "resc_net" || key == "RESC_LOC") resc.location = val;
                else if (key == "resource_property_path" || key == "resc_def_path" || key == "RESC_VAULT_PATH") resc.vault_path = val;
                else if (key == "resource_property_comment" || key == "r_comment" || key == "RESC_COMMENT") resc.comments = val;
                else if (key == "resource_property_status" || key == "resc_status" || key == "RESC_STATUS") resc.status = val.empty() ? 0 : std::stoi(val);
                else if (key == "resource_property_context" || key == "resc_context" || key == "RESC_CONTEXT") resc.context = val;
                else if (key == "resource_property_create_time" || key == "create_ts" || key == "RESC_CREATE_TIME") resc.create_ts = val;
                else if (key == "resource_property_modify_time" || key == "modify_ts" || key == "RESC_MODIFY_TIME") resc.modify_ts = val;
            }
        }
        
        if (resc.id <= 0) g_catalog->get_next_sequence_value("R_RESC_MAIN", resc.id);
        
        irods::catalog::resc_id_t out_id;
        auto ret = g_catalog->register_resource(resc, out_id);
        rodsLog(LOG_NOTICE, "L3_PLUGIN: EXITING db_reg_resc_op SUCCESS");
        return ret;
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_reg_resc_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

irods::error db_mod_resc_op(irods::plugin_context& _ctx, const char* _resc, const char* _prop, const char* _val) {
    try {
        rodsLog(LOG_NOTICE, "L3_PLUGIN: ENTERING db_mod_resc_op");
        irods::catalog::resc_id_t rid = 0;
        if (_resc && g_catalog->resolve_resource_name(_resc, rid).ok()) {
            auto ret = g_catalog->modify_resource(rid, safe_string(_prop), safe_string(_val));
            rodsLog(LOG_NOTICE, "L3_PLUGIN: EXITING db_mod_resc_op SUCCESS");
            return ret;
        }
        rodsLog(LOG_NOTICE, "L3_PLUGIN: EXITING db_mod_resc_op SUCCESS (no resc)");
        return SUCCESS();
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_mod_resc_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

irods::error db_del_resc_op(irods::plugin_context& _ctx, const char* _resc, int _unused) {
    try {
        rodsLog(LOG_NOTICE, "L3_PLUGIN: ENTERING db_del_resc_op");
        irods::catalog::resc_id_t rid = 0;
        if (_resc && g_catalog->resolve_resource_name(_resc, rid).ok()) {
            auto ret = g_catalog->delete_resource(rid);
            rodsLog(LOG_NOTICE, "L3_PLUGIN: EXITING db_del_resc_op SUCCESS");
            return ret;
        }
        rodsLog(LOG_NOTICE, "L3_PLUGIN: EXITING db_del_resc_op SUCCESS (no resc)");
        return SUCCESS();
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_del_resc_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

irods::error db_add_child_resc_op(irods::plugin_context& _ctx, const char* _parent, const char* _child, const char* _context) {
    try {
        rodsLog(LOG_NOTICE, "L3_PLUGIN: ENTERING db_add_child_resc_op");
        auto ret = g_catalog->add_child_resource(safe_string(_parent), safe_string(_child), safe_string(_context));
        rodsLog(LOG_NOTICE, "L3_PLUGIN: EXITING db_add_child_resc_op SUCCESS");
        return ret;
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_add_child_resc_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

irods::error db_del_child_resc_op(irods::plugin_context& _ctx, const char* _parent, const char* _child) {
    try {
        rodsLog(LOG_NOTICE, "L3_PLUGIN: ENTERING db_del_child_resc_op");
        auto ret = g_catalog->remove_child_resource(safe_string(_parent), safe_string(_child));
        rodsLog(LOG_NOTICE, "L3_PLUGIN: EXITING db_del_child_resc_op SUCCESS");
        return ret;
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_del_child_resc_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

irods::error db_get_hierarchy_for_resc_op(irods::plugin_context& _ctx, const char* _resc_name, char** _hier) {
    try {
        rodsLog(LOG_NOTICE, "L3_PLUGIN: ENTERING db_get_hierarchy_for_resc_op");
        if (!_resc_name || !_hier) return ERROR(SYS_INVALID_INPUT_PARAM, "Null input");
        std::string hier;
        auto ret = g_catalog->get_hierarchy_for_resource(_resc_name, hier);
        if (ret.ok()) *_hier = strdup(hier.c_str());
        rodsLog(LOG_NOTICE, "L3_PLUGIN: EXITING db_get_hierarchy_for_resc_op SUCCESS");
        return ret;
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_get_hierarchy_for_resc_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

irods::error db_update_resc_obj_count(irods::plugin_context& _ctx, const std::string* _resc_name, int _delta) {
    try {
        rodsLog(LOG_NOTICE, "L3_PLUGIN: ENTERING db_update_resc_obj_count");
        if (!_resc_name) return ERROR(SYS_INVALID_INPUT_PARAM, "Null resource name");
        irods::catalog::snowflake_id_t rid = 0;
        if (!g_catalog->resolve_resource_name(*_resc_name, rid).ok()) return SUCCESS();
        auto ret = g_catalog->update_resource_object_count(rid, _delta);
        rodsLog(LOG_NOTICE, "L3_PLUGIN: EXITING db_update_resc_obj_count SUCCESS");
        return ret;
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_update_resc_obj_count EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

irods::error db_mod_resc_data_paths_op(irods::plugin_context& _ctx, const char* _resc, const char* _old_path, const char* _new_path, const char* _user) {
    rodsLog(LOG_NOTICE, "L3_PLUGIN: ENTERING db_mod_resc_data_paths_op resc [%s] old [%s] new [%s] user [%s]", safe_string(_resc).c_str(), safe_string(_old_path).c_str(), safe_string(_new_path).c_str(), safe_string(_user).c_str());
    return SUCCESS();
}

irods::error db_mod_resc_freespace_op(irods::plugin_context& _ctx, const char* _resc, const char* _freespace) {
    rodsLog(LOG_NOTICE, "L3_PLUGIN: ENTERING db_mod_resc_freespace_op resc [%s] freespace [%s]", safe_string(_resc).c_str(), safe_string(_freespace).c_str());
    return SUCCESS();
}

// Identity
irods::error db_reg_user_op(irods::plugin_context& _ctx, const irods::catalog::user& _user, irods::catalog::user_id_t* _out_id) {
    try {
        rodsLog(LOG_NOTICE, "L3_PLUGIN: ENTERING db_reg_user_op");
        if (!_out_id) return ERROR(SYS_INVALID_INPUT_PARAM, "null user_id_t pointer");
        auto ret = g_catalog->register_user(_user, *_out_id);
        rodsLog(LOG_NOTICE, "L3_PLUGIN: EXITING db_reg_user_op SUCCESS");
        return ret;
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_reg_user_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

irods::error db_reg_user_re_op(irods::plugin_context& _ctx, userInfo_t* _info) {
    try {
        rodsLog(LOG_NOTICE, "L3_PLUGIN: ENTERING db_reg_user_re_op");
        if (!_info) return ERROR(SYS_INVALID_INPUT_PARAM, "null userInfo_t");
        
        irods::catalog::user user;
        user.name = safe_string(_info->userName);
        user.type = safe_string(_info->userType);
        user.zone = safe_string(_info->rodsZone);
        if (user.zone.empty()) {
            user.zone = irods::server_properties::instance().map().get_json().at(KW_CFG_ZONE_NAME).get<std::string>();
        }

        // Ensure a unique ID for the new user
        g_catalog->get_next_sequence_value("R_USER_MAIN", user.id);

        rodsLog(LOG_NOTICE, "L3_PLUGIN: db_reg_user_re_op: registering user [%s] type [%s] zone [%s] id [%lu]", user.name.c_str(), user.type.c_str(), user.zone.c_str(), user.id);
        
        irods::catalog::user_id_t out_id;
        auto ret = g_catalog->register_user(user, out_id);
        
        rodsLog(LOG_NOTICE, "L3_PLUGIN: EXITING db_reg_user_re_op SUCCESS");
        return ret;
    } catch (const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_reg_user_re_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

irods::error db_mod_user_op(irods::plugin_context& _ctx, const char* _user, const char* _option, const char* _value) {
    try {
        rodsLog(LOG_NOTICE, "L3_PLUGIN: ENTERING db_mod_user_op");
        auto ret = g_catalog->modify_user(safe_string(_user), safe_string(_option), safe_string(_value));
        rodsLog(LOG_NOTICE, "L3_PLUGIN: EXITING db_mod_user_op SUCCESS");
        return ret;
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_mod_user_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

irods::error db_del_user_op(irods::plugin_context& _ctx, const char* _username) {
    try {
        rodsLog(LOG_NOTICE, "L3_PLUGIN: ENTERING db_del_user_op");
        auto ret = g_catalog->delete_user(safe_string(_username));
        rodsLog(LOG_NOTICE, "L3_PLUGIN: EXITING db_del_user_op SUCCESS");
        return ret;
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_del_user_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

irods::error db_del_user_re_op(irods::plugin_context& _ctx, userInfo_t* _info) {
    try {
        rodsLog(LOG_NOTICE, "L3_PLUGIN: ENTERING db_del_user_re_op");
        if (!_info) return ERROR(SYS_INVALID_INPUT_PARAM, "null userInfo_t");
        
        std::string username = safe_string(_info->userName);
        rodsLog(LOG_NOTICE, "L3_PLUGIN: db_del_user_re_op: deleting user [%s]", username.c_str());
        
        auto ret = g_catalog->delete_user(username);
        
        rodsLog(LOG_NOTICE, "L3_PLUGIN: EXITING db_del_user_re_op SUCCESS");
        return ret;
    } catch (const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_del_user_re_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

irods::error db_check_auth_op(irods::plugin_context& _ctx, const char* _scheme, const char* _challenge, const char* _response, const char* _user_name, int* _user_priv_level, int* _client_priv_level) {
    try {
        rodsLog(LOG_NOTICE, "L3_PLUGIN: ENTERING db_check_auth_op");
        if (!_user_name) return ERROR(SYS_INVALID_INPUT_PARAM, "Null username");
        std::string user_str(_user_name);
        std::string user_name = user_str, zone_name = "";
        auto pos = user_str.find('#');
        if (pos != std::string::npos) { user_name = user_str.substr(0, pos); zone_name = user_str.substr(pos + 1); }
        else { zone_name = irods::server_properties::instance().map().get_json().at(KW_CFG_ZONE_NAME).get<std::string>(); }
        
        auto ret = g_catalog->check_auth(user_name, zone_name, *_user_priv_level);
        if (ret.ok()) {
            // Map internal privilege level back to iRODS core expectations
            // 5 -> LOCAL_PRIV_USER_AUTH
            // 1 -> LOCAL_USER_AUTH (3)
            if (*_user_priv_level == 1) {
                *_user_priv_level = 3; // LOCAL_USER_AUTH
            } else if (*_user_priv_level == 5) {
                *_user_priv_level = 5; // LOCAL_PRIV_USER_AUTH
            }
            *_client_priv_level = *_user_priv_level;
            rodsLog(LOG_NOTICE, "L3_PLUGIN: EXITING db_check_auth_op SUCCESS (priv=%d)", *_user_priv_level);
        } else {
            rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_check_auth_op FAILED: %s", ret.result().c_str());
        }
        return ret;
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_check_auth_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

irods::error db_check_auth_credentials_op(irods::plugin_context& _ctx, const char* _username, const char* _zone, const char* _password, int* _correct) {
    try {
        rodsLog(LOG_NOTICE, "L3_PLUGIN: ENTERING db_check_auth_credentials_op");
        bool correct = false;
        auto ret = g_catalog->check_auth_credentials(safe_string(_username), safe_string(_zone), safe_string(_password), correct);
        *_correct = correct ? 1 : 0;
        rodsLog(LOG_NOTICE, "L3_PLUGIN: EXITING db_check_auth_credentials_op SUCCESS");
        return ret;
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_check_auth_credentials_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

irods::error db_mod_group_op(irods::plugin_context& _ctx, const char* _group, const char* _option, const char* _user, const char* _zone) {
    std::string zone_str = safe_string(_zone);
    if (zone_str.empty()) {
        const auto& config = irods::server_properties::instance().map().get_json();
        zone_str = config.at(KW_CFG_ZONE_NAME).get<std::string>();
    }
    rodsLog(LOG_NOTICE, "L3_PLUGIN: ENTERING db_mod_group_op group [%s] opt [%s] user [%s] zone [%s]", safe_string(_group).c_str(), safe_string(_option).c_str(), safe_string(_user).c_str(), zone_str.c_str());
    try {
        if (_option && std::string(_option) == "add") {
            auto ret = g_catalog->add_user_to_group(safe_string(_user), zone_str, safe_string(_group));
            rodsLog(LOG_NOTICE, "L3_PLUGIN: EXITING db_mod_group_op SUCCESS");
            return ret;
        } else if (_option && std::string(_option) == "remove") {
            auto ret = g_catalog->remove_user_from_group(safe_string(_user), zone_str, safe_string(_group));
            rodsLog(LOG_NOTICE, "L3_PLUGIN: EXITING db_mod_group_op SUCCESS");
            return ret;
        }
        rodsLog(LOG_NOTICE, "L3_PLUGIN: EXITING db_mod_group_op SUCCESS (no opt)");
        return SUCCESS();
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_mod_group_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

// Metadata
irods::error db_add_avu_metadata_op(irods::plugin_context& _ctx, const char* _type, const char* _target_id, const char* _attr, const char* _val, const char* _units, const KeyValPair* _unused) {
    try {
        rodsLog(LOG_NOTICE, "L3_PLUGIN: ENTERING db_add_avu_metadata_op");
        auto ret = g_catalog->add_avu_metadata(safe_string(_type), safe_string(_target_id), {safe_string(_attr), safe_string(_val), safe_string(_units)});
        rodsLog(LOG_NOTICE, "L3_PLUGIN: EXITING db_add_avu_metadata_op SUCCESS");
        return ret;
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_add_avu_metadata_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

irods::error db_del_avu_metadata_op(irods::plugin_context& _ctx, const char* _type, const char* _target_id, const char* _attr, const char* _val, const char* _units, const KeyValPair* _unused) {
    try {
        rodsLog(LOG_NOTICE, "L3_PLUGIN: ENTERING db_del_avu_metadata_op");
        auto ret = g_catalog->delete_avu_metadata(safe_string(_type), safe_string(_target_id), {safe_string(_attr), safe_string(_val), safe_string(_units)});
        rodsLog(LOG_NOTICE, "L3_PLUGIN: EXITING db_del_avu_metadata_op SUCCESS");
        return ret;
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_del_avu_metadata_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

irods::error db_mod_avu_metadata_op(irods::plugin_context& _ctx, const char* _type, const char* _target_id, const char* _old_attr, const char* _old_val, const char* _old_units, const char* _new_attr, const char* _new_val, const char* _new_units, const KeyValPair* _unused) {
    try {
        rodsLog(LOG_NOTICE, "L3_PLUGIN: ENTERING db_mod_avu_metadata_op");
        auto ret = g_catalog->modify_avu_metadata(safe_string(_type), safe_string(_target_id), {safe_string(_old_attr), safe_string(_old_val), safe_string(_old_units)}, {safe_string(_new_attr), safe_string(_new_val), safe_string(_new_units)});
        rodsLog(LOG_NOTICE, "L3_PLUGIN: EXITING db_mod_avu_metadata_op SUCCESS");
        return ret;
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_mod_avu_metadata_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

irods::error db_copy_avu_metadata_op(irods::plugin_context& _ctx, const char* _src_type, const char* _src_id, const char* _dst_type, const char* _dst_id) {
    try {
        rodsLog(LOG_NOTICE, "L3_PLUGIN: ENTERING db_copy_avu_metadata_op");
        auto ret = g_catalog->copy_avu_metadata(safe_string(_src_type), safe_string(_src_id), safe_string(_dst_type), safe_string(_dst_id));
        rodsLog(LOG_NOTICE, "L3_PLUGIN: EXITING db_copy_avu_metadata_op SUCCESS");
        return ret;
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_copy_avu_metadata_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

irods::error db_set_avu_metadata_op(irods::plugin_context& _ctx, const char* _type, const char* _target_id, const char* _attr, const char* _val, const char* _units, const KeyValPair* _unused) {
    try {
        rodsLog(LOG_NOTICE, "L3_PLUGIN: ENTERING db_set_avu_metadata_op");
        auto ret = g_catalog->set_avu_metadata(safe_string(_type), safe_string(_target_id), {safe_string(_attr), safe_string(_val), safe_string(_units)});
        rodsLog(LOG_NOTICE, "L3_PLUGIN: EXITING db_set_avu_metadata_op SUCCESS");
        return ret;
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_set_avu_metadata_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

irods::error db_mod_access_control_op(irods::plugin_context& _ctx, int _recursive, const char* _access_level, const char* _user, const char* _zone, const char* _path) {
    try {
        rodsLog(LOG_NOTICE, "L3_PLUGIN: ENTERING db_mod_access_control_op");
        auto ret = g_catalog->set_access(safe_string(_user), safe_string(_zone), safe_string(_path), safe_string(_access_level), _recursive != 0);
        rodsLog(LOG_NOTICE, "L3_PLUGIN: EXITING db_mod_access_control_op SUCCESS");
        return ret;
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_mod_access_control_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

irods::error db_check_permission_to_modify_data_object_op(irods::plugin_context& _ctx, rodsLong_t _data_id) {
    try {
        rodsLog(LOG_NOTICE, "L3_PLUGIN: ENTERING db_check_permission_to_modify_data_object_op id [%ld]", _data_id);
        bool allowed = false;
        
        std::string user_name = _ctx.comm()->clientUser.userName;
        irods::catalog::snowflake_id_t usid = 0;
        if (g_catalog->resolve_user_name(user_name, usid).ok()) {
            irods::catalog::snowflake_id_t dsid = g_catalog->make_id(irods::catalog::EntityType::DataObject, (uint64_t)_data_id);
            g_catalog->check_permission_to_modify_data_object(usid, dsid, allowed);
        }

        rodsLog(LOG_NOTICE, "L3_PLUGIN: EXITING db_check_permission_to_modify_data_object_op id [%ld] allowed [%d]", _data_id, allowed);
        if (allowed) return SUCCESS();
        rodsLog(LOG_ERROR, "L3_PLUGIN: Access Denied for user [%s] on object [%ld]", user_name.c_str(), _data_id);
        return ERROR(CAT_NO_ACCESS_PERMISSION, "User does not have permission to modify data object");
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_check_permission_to_modify_data_object_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

irods::error db_update_ticket_write_byte_count_op(irods::plugin_context& _ctx, rodsLong_t _data_id, rodsLong_t _bytes) {
    rodsLog(LOG_NOTICE, "L3_PLUGIN: ENTERING db_update_ticket_write_byte_count_op id [%ld] bytes [%ld]", _data_id, _bytes);
    rodsLog(LOG_NOTICE, "L3_PLUGIN: EXITING db_update_ticket_write_byte_count_op SUCCESS");
    return SUCCESS();
}

irods::error db_data_object_finalize_op(irods::plugin_context& _ctx, const char* _path) {
    try {
        std::string json_str = safe_string(_path);
        rodsLog(LOG_NOTICE, "L3_PLUGIN: ENTERING db_data_object_finalize_op [%s]", json_str.c_str());
        
        if (json_str.empty()) return SUCCESS();

        auto j = nlohmann::json::parse(json_str);
        if (j.contains("replicas") && j["replicas"].is_array()) {
            for (auto& r : j["replicas"]) {
                if (r.contains("after")) {
                    auto& after = r["after"];
                    uint64_t data_id = std::stoull(after.value("data_id", "0"));
                    uint32_t repl_num = std::stoul(after.value("data_repl_num", "0"));
                    uint64_t resc_id = std::stoull(after.value("resc_id", "0"));
                    uint64_t data_size = std::stoull(after.value("data_size", "0"));
                    std::string checksum = after.value("data_checksum", "");
                    std::string modify_ts = after.value("modify_ts", "");

                    irods::catalog::replica repl;
                    repl.data_id = data_id;
                    repl.replica_number = repl_num;
                    repl.resource_id = resc_id;
                    repl.physical_path = after.value("data_path", "");
                    repl.resc_hier = after.value("resc_hier", "");
                    repl.status = after.value("data_is_dirty", "1");
                    repl.checksum = checksum;
                    repl.modify_ts = get_timestamp(modify_ts);

                    // Update the replica in the catalog
                    g_catalog->register_replica(repl);

                    // Update data object size
                    g_catalog->modify_data_object(data_id, "DATA_SIZE", std::to_string(data_size));
                }
            }
        }

        rodsLog(LOG_NOTICE, "L3_PLUGIN: EXITING db_data_object_finalize_op SUCCESS");
        return SUCCESS();
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_data_object_finalize_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

irods::error db_get_delay_rule_info_op(irods::plugin_context& _ctx, const char* _rule_id, std::vector<std::string>* _out_info) {
    rodsLog(LOG_NOTICE, "L3_PLUGIN: ENTERING db_get_delay_rule_info_op [%s]", safe_string(_rule_id).c_str());
    rodsLog(LOG_NOTICE, "L3_PLUGIN: EXITING db_get_delay_rule_info_op SUCCESS");
    return SUCCESS();
}

irods::error db_delay_rule_lock_op(irods::plugin_context& _ctx, const char* _rule_id, const char* _lock_id) {
    rodsLog(LOG_NOTICE, "L3_PLUGIN: ENTERING db_delay_rule_lock_op rule [%s] lock [%s]", safe_string(_rule_id).c_str(), safe_string(_lock_id).c_str());
    rodsLog(LOG_NOTICE, "L3_PLUGIN: EXITING db_delay_rule_lock_op SUCCESS");
    return SUCCESS();
}

irods::error db_delay_rule_unlock_op(irods::plugin_context& _ctx, const char* _rule_id, const char* _lock_id) {
    rodsLog(LOG_NOTICE, "L3_PLUGIN: ENTERING db_delay_rule_unlock_op rule [%s] lock [%s]", safe_string(_rule_id).c_str(), safe_string(_lock_id).c_str());
    rodsLog(LOG_NOTICE, "L3_PLUGIN: EXITING db_delay_rule_unlock_op SUCCESS");
    return SUCCESS();
}

irods::error db_check_password_op(irods::plugin_context& _ctx, const char* _user, const char* _zone, const char* _password, int* _correct) {
    rodsLog(LOG_NOTICE, "L3_PLUGIN: ENTERING db_check_password_op user [%s] zone [%s]", safe_string(_user).c_str(), safe_string(_zone).c_str());
    if (_correct) *_correct = 0;
    rodsLog(LOG_NOTICE, "L3_PLUGIN: EXITING db_check_password_op SUCCESS");
    return SUCCESS();
}

irods::error db_make_session_token_op(irods::plugin_context& _ctx, const char* _user, const char* _zone, char* _token) {
    rodsLog(LOG_NOTICE, "L3_PLUGIN: ENTERING db_make_session_token_op user [%s] zone [%s]", safe_string(_user).c_str(), safe_string(_zone).c_str());
    rodsLog(LOG_NOTICE, "L3_PLUGIN: EXITING db_make_session_token_op SUCCESS");
    return SUCCESS();
}

irods::error db_check_session_token_op(irods::plugin_context& _ctx, const char* _user, const char* _zone, const char* _token, int* _correct) {
    rodsLog(LOG_NOTICE, "L3_PLUGIN: ENTERING db_check_session_token_op user [%s] zone [%s]", safe_string(_user).c_str(), safe_string(_zone).c_str());
    if (_correct) *_correct = 0;
    rodsLog(LOG_NOTICE, "L3_PLUGIN: EXITING db_check_session_token_op SUCCESS");
    return SUCCESS();
}

irods::error db_remove_session_tokens_op(irods::plugin_context& _ctx, const char* _user, const char* _zone) {
    rodsLog(LOG_NOTICE, "L3_PLUGIN: ENTERING db_remove_session_tokens_op user [%s] zone [%s]", safe_string(_user).c_str(), safe_string(_zone).c_str());
    rodsLog(LOG_NOTICE, "L3_PLUGIN: EXITING db_remove_session_tokens_op SUCCESS");
    return SUCCESS();
}

irods::error db_remove_password_op(irods::plugin_context& _ctx, const char* _user, const char* _zone) {
    rodsLog(LOG_NOTICE, "L3_PLUGIN: ENTERING db_remove_password_op user [%s] zone [%s]", safe_string(_user).c_str(), safe_string(_zone).c_str());
    rodsLog(LOG_NOTICE, "L3_PLUGIN: EXITING db_remove_password_op SUCCESS");
    return SUCCESS();
}

// Zones
irods::error db_reg_zone_op(irods::plugin_context& _ctx, const char* _zone, const char* _type, const char* _conn, const char* _comment) {
    try {
        rodsLog(LOG_NOTICE, "L3_PLUGIN: ENTERING db_reg_zone_op");
        irods::catalog::zone z;
        z.name = safe_string(_zone);
        z.type = safe_string(_type);
        z.connection = safe_string(_conn);
        z.comment = safe_string(_comment);
        auto ret = g_catalog->register_zone(z);
        rodsLog(LOG_NOTICE, "L3_PLUGIN: EXITING db_reg_zone_op SUCCESS");
        return ret;
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_reg_zone_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

irods::error db_mod_zone_op(irods::plugin_context& _ctx, const char* _zone, const char* _prop, const char* _val) {
    try {
        rodsLog(LOG_NOTICE, "L3_PLUGIN: ENTERING db_mod_zone_op");
        auto ret = g_catalog->modify_zone(safe_string(_zone), safe_string(_prop), safe_string(_val));
        rodsLog(LOG_NOTICE, "L3_PLUGIN: EXITING db_mod_zone_op SUCCESS");
        return ret;
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_mod_zone_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

irods::error db_del_zone_op(irods::plugin_context& _ctx, const char* _zone) {
    try {
        rodsLog(LOG_NOTICE, "L3_PLUGIN: ENTERING db_del_zone_op");
        auto ret = g_catalog->delete_zone(safe_string(_zone));
        rodsLog(LOG_NOTICE, "L3_PLUGIN: EXITING db_del_zone_op SUCCESS");
        return ret;
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_del_zone_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

// Token
irods::error db_reg_token_op(irods::plugin_context& _ctx, const char* _name, const char* _value, const char* _namespace) {
    try {
        rodsLog(LOG_NOTICE, "L3_PLUGIN: ENTERING db_reg_token_op");
        auto ret = g_catalog->register_token(safe_string(_name), safe_string(_value), safe_string(_namespace));
        rodsLog(LOG_NOTICE, "L3_PLUGIN: EXITING db_reg_token_op SUCCESS");
        return ret;
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_reg_token_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

irods::error db_del_token_op(irods::plugin_context& _ctx, const char* _name, const char* _namespace) {
    try {
        rodsLog(LOG_NOTICE, "L3_PLUGIN: ENTERING db_del_token_op");
        auto ret = g_catalog->delete_token(safe_string(_name), safe_string(_namespace));
        rodsLog(LOG_NOTICE, "L3_PLUGIN: EXITING db_del_token_op SUCCESS");
        return ret;
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_del_token_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

// Quota
irods::error db_set_quota_op(irods::plugin_context& _ctx, const char* _user, const char* _resc, rodsLong_t _limit) {
    try {
        rodsLog(LOG_NOTICE, "L3_PLUGIN: ENTERING db_set_quota_op");
        auto ret = g_catalog->set_quota(safe_string(_user), safe_string(_resc), _limit);
        rodsLog(LOG_NOTICE, "L3_PLUGIN: EXITING db_set_quota_op SUCCESS");
        return ret;
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_set_quota_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

irods::error db_check_quota_op(irods::plugin_context& _ctx, const char* _user, const char* _resc, rodsLong_t* _usage, int* _limit_exceeded) {
    try {
        rodsLog(LOG_NOTICE, "L3_PLUGIN: ENTERING db_check_quota_op");
        if (!_usage || !_limit_exceeded) return ERROR(SYS_INVALID_INPUT_PARAM, "Null inputs");
        int64_t usage = 0, limit = -1;
        auto ret = g_catalog->check_quota(safe_string(_user), safe_string(_resc), usage, limit);
        if (ret.ok()) { *_usage = usage; *_limit_exceeded = (limit >= 0 && usage > limit) ? 1 : 0; }
        rodsLog(LOG_NOTICE, "L3_PLUGIN: EXITING db_check_quota_op SUCCESS");
        return ret;
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_check_quota_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

irods::error db_calc_usage_and_quota_op(irods::plugin_context& _ctx) { return SUCCESS(); }

// Rules
irods::error db_reg_rule_exec_op(irods::plugin_context& _ctx, ruleExecSubmitInp_t* _info) {
    try {
        rodsLog(LOG_NOTICE, "L3_PLUGIN: ENTERING db_reg_rule_exec_op");
        if (!_info) return ERROR(SYS_INVALID_INPUT_PARAM, "null ruleExecSubmitInp_t");
        irods::catalog::rule_exec re;
        re.id = 0; 
        re.name = _info->ruleName; 
        re.exec_time = _info->exeTime; 
        re.priority = _info->priority;
        g_catalog->get_next_sequence_value("R_RULE_EXEC", re.id);
        uint64_t out_id;
        auto ret = g_catalog->register_rule_execution(re, out_id);
        rodsLog(LOG_NOTICE, "L3_PLUGIN: EXITING db_reg_rule_exec_op SUCCESS");
        return ret;
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_reg_rule_exec_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

irods::error db_del_rule_exec_op(irods::plugin_context& _ctx, const char* _rule_id) {
    try {
        rodsLog(LOG_NOTICE, "L3_PLUGIN: ENTERING db_del_rule_exec_op");
        std::string rid_str = safe_string(_rule_id);
        auto ret = g_catalog->delete_rule_execution(rid_str.empty() ? 0 : std::stoull(rid_str));
        rodsLog(LOG_NOTICE, "L3_PLUGIN: EXITING db_del_rule_exec_op SUCCESS");
        return ret;
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_del_rule_exec_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

// Specific Query
irods::error db_add_specific_query_op(irods::plugin_context& _ctx, const char* _alias, const char* _sql) {
    try {
        rodsLog(LOG_NOTICE, "L3_PLUGIN: ENTERING db_add_specific_query_op");
        auto ret = g_catalog->register_specific_query(safe_string(_alias), safe_string(_sql));
        rodsLog(LOG_NOTICE, "L3_PLUGIN: EXITING db_add_specific_query_op SUCCESS");
        return ret;
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_add_specific_query_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

irods::error db_del_specific_query_op(irods::plugin_context& _ctx, const char* _alias) {
    try {
        rodsLog(LOG_NOTICE, "L3_PLUGIN: ENTERING db_del_specific_query_op");
        auto ret = g_catalog->delete_specific_query(safe_string(_alias));
        rodsLog(LOG_NOTICE, "L3_PLUGIN: EXITING db_del_specific_query_op SUCCESS");
        return ret;
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_del_specific_query_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

// Logical Quota
irods::error db_set_logical_quota_op(irods::plugin_context& _ctx, const char* _coll_name, rodsLong_t _limit) {
    try {
        rodsLog(LOG_NOTICE, "L3_PLUGIN: ENTERING db_set_logical_quota_op");
        auto ret = g_catalog->set_logical_quota(safe_string(_coll_name), _limit);
        rodsLog(LOG_NOTICE, "L3_PLUGIN: EXITING db_set_logical_quota_op SUCCESS");
        return ret;
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_set_logical_quota_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

irods::error db_check_logical_quota_op(irods::plugin_context& _ctx, const char* _coll_name, std::vector<std::tuple<std::string, std::int64_t, std::int64_t, std::int64_t, std::int64_t>>* _quota_values) {
    try {
        rodsLog(LOG_NOTICE, "L3_PLUGIN: ENTERING db_check_logical_quota_op");
        if (!_coll_name || !_quota_values) return ERROR(SYS_INVALID_INPUT_PARAM, "Null inputs");
        int64_t usage = 0, limit = -1;
        auto ret = g_catalog->check_logical_quota(safe_string(_coll_name), usage, limit);
        if (ret.ok()) _quota_values->push_back(std::make_tuple(safe_string(_coll_name), usage, limit, 0, 0));
        rodsLog(LOG_NOTICE, "L3_PLUGIN: EXITING db_check_logical_quota_op SUCCESS");
        return ret;
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_check_logical_quota_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

irods::error db_calc_logical_usage_and_quota_op(irods::plugin_context& _ctx, const char* _coll_name, rodsLong_t* _usage, rodsLong_t* _limit) {
    try {
        rodsLog(LOG_NOTICE, "L3_PLUGIN: ENTERING db_calc_logical_usage_and_quota_op");
        int64_t usage = 0, limit = -1;
        auto ret = g_catalog->calculate_logical_usage(safe_string(_coll_name), usage);
        if (ret.ok()) ret = g_catalog->check_logical_quota(safe_string(_coll_name), usage, limit);
        if (_usage) *_usage = usage; if (_limit) *_limit = limit;
        rodsLog(LOG_NOTICE, "L3_PLUGIN: EXITING db_calc_logical_usage_and_quota_op SUCCESS");
        return ret;
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_calc_logical_usage_and_quota_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

// Server Load
irods::error db_reg_server_load_op(irods::plugin_context& _ctx, const char* _host, int _load) { return SUCCESS(); }
irods::error db_purge_server_load_op(irods::plugin_context& _ctx, const char* _host) { return SUCCESS(); }

// Grid Config
irods::error db_set_grid_configuration_value_op(irods::plugin_context& _ctx, const char* _ns, const char* _name, const char* _value) {
    try {
        rodsLog(LOG_NOTICE, "L3_PLUGIN: ENTERING db_set_grid_configuration_value_op");
        if (auto ret = init_l3kvg_catalog(); !ret.ok()) return ret;
        std::string full_name = safe_string(_ns) + ":" + safe_string(_name);
        auto ret = g_catalog->set_grid_configuration_value(full_name, safe_string(_value));
        rodsLog(LOG_NOTICE, "L3_PLUGIN: EXITING db_set_grid_configuration_value_op SUCCESS");
        return ret;
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_set_grid_configuration_value_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

irods::error db_get_grid_configuration_value_op(irods::plugin_context& _ctx, const char* _ns, const char* _name, char* _value, std::size_t _value_buf_size) {
    try {
        rodsLog(LOG_NOTICE, "L3_PLUGIN: ENTERING db_get_grid_configuration_value_op");
        if (auto ret = init_l3kvg_catalog(); !ret.ok()) return ret;
        std::string val;
        std::string full_name = safe_string(_ns) + ":" + safe_string(_name);
        auto ret = g_catalog->get_grid_configuration_value(full_name, val);
        if (ret.ok()) strncpy(_value, val.c_str(), _value_buf_size - 1);
        rodsLog(LOG_NOTICE, "L3_PLUGIN: EXITING db_get_grid_configuration_value_op SUCCESS");
        return ret;
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_get_grid_configuration_value_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

namespace irods::catalog::bridge {
    irods::experimental::genquery2::select synthesize_gq2_ast(genQueryInp_t* _inp, irods::catalog::CatalogFacade* _catalog, std::vector<uint64_t>& _starting_nodes);
    void pack_gq1_results(const irods::catalog::ResultSet& _results, genQueryInp_t* _inp, genQueryOut_t* _out);
}

// GenQuery
irods::error db_get_catalog_version_op(irods::plugin_context& _ctx, int* _version) {
    rodsLog(LOG_NOTICE, "L3_PLUGIN: ENTERING db_get_catalog_version_op");
    try {
        if (!_version) return ERROR(SYS_INVALID_INPUT_PARAM, "Null version pointer");
        *_version = 2; 
        rodsLog(LOG_NOTICE, "L3_PLUGIN: EXITING db_get_catalog_version_op SUCCESS");
        return SUCCESS();

    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_get_catalog_version_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

irods::error db_initialize_catalog_op(irods::plugin_context& _ctx) {
    try {
        rodsLog(LOG_NOTICE, "L3_PLUGIN: ENTERING db_initialize_catalog_op");
        if (auto ret = init_l3kvg_catalog(); !ret.ok()) return ret;
        const auto& config = irods::server_properties::instance().map().get_json();
        auto ret = g_catalog->bootstrap_catalog(config.at(KW_CFG_ZONE_NAME).get<std::string>(), config.at(KW_CFG_ZONE_USER).get<std::string>());
        rodsLog(LOG_NOTICE, "L3_PLUGIN: EXITING db_initialize_catalog_op SUCCESS");
        return ret;
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_initialize_catalog_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

irods::error db_gen_query_op(irods::plugin_context& _ctx, genQueryInp_t* _inp, genQueryOut_t* _out) {
    try {
        rodsLog(LOG_NOTICE, "L3_PLUGIN: ENTERING db_gen_query_op");
        if (auto ret = init_l3kvg_catalog(); !ret.ok()) return ret;
        if (!_inp || !_out) return ERROR(SYS_INTERNAL_NULL_INPUT_ERR, "Null input/output");
        std::vector<uint64_t> starting_nodes;
        auto ast = irods::catalog::bridge::synthesize_gq2_ast(_inp, g_catalog.get(), starting_nodes);
        irods::catalog::ResultSet results;
        auto ret = g_catalog->execute_query(ast, results, starting_nodes);
        if (!ret.ok()) return ret;
        irods::catalog::bridge::pack_gq1_results(results, _inp, _out);
        if (_out->rowCnt <= 0) {
            rodsLog(LOG_NOTICE, "L3_PLUGIN: EXITING db_gen_query_op SUCCESS (NO ROWS)");
            return ERROR(CAT_NO_ROWS_FOUND, "No rows found");
        }
        rodsLog(LOG_NOTICE, "L3_PLUGIN: EXITING db_gen_query_op SUCCESS");
        return SUCCESS();
    } catch (const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_gen_query_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

class l3kvg_database_plugin : public irods::database {
public:
    l3kvg_database_plugin(const std::string& _inst, const std::string& _ctx) : irods::database(_inst, _ctx) {
        set_start_operation(db_maintenance_op);
        add_operation(irods::DATABASE_OP_START, std::function<irods::error(irods::plugin_context&)>(db_start_op));
        add_operation(irods::DATABASE_OP_STOP, std::function<irods::error(irods::plugin_context&)>(db_stop_op));
        add_operation(irods::DATABASE_OP_OPEN, std::function<irods::error(irods::plugin_context&)>(db_open_op));
        add_operation(irods::DATABASE_OP_CLOSE, std::function<irods::error(irods::plugin_context&)>(db_close_op));
        add_operation(irods::DATABASE_OP_COMMIT, std::function<irods::error(irods::plugin_context&)>(db_commit_op));
        add_operation(irods::DATABASE_OP_ROLLBACK, std::function<irods::error(irods::plugin_context&)>(db_rollback_op));
        
        add_operation<dataObjInfo_t*>(irods::DATABASE_OP_REG_DATA_OBJ, std::function<irods::error(irods::plugin_context&, dataObjInfo_t*)>(db_reg_data_obj_op));
        add_operation<dataObjInfo_t*, keyValPair_t*>(irods::DATABASE_OP_MOD_DATA_OBJ_META, std::function<irods::error(irods::plugin_context&, dataObjInfo_t*, keyValPair_t*)>(db_mod_data_obj_meta_op));
        add_operation<rodsLong_t, const char*>(irods::DATABASE_OP_RENAME_OBJECT, std::function<irods::error(irods::plugin_context&, rodsLong_t, const char*)>(db_rename_object_op));
        add_operation<rodsLong_t, rodsLong_t>(irods::DATABASE_OP_MOVE_OBJECT, std::function<irods::error(irods::plugin_context&, rodsLong_t, rodsLong_t)>(db_move_object_op));

        add_operation<dataObjInfo_t*, dataObjInfo_t*, keyValPair_t*>(irods::DATABASE_OP_REG_REPLICA, std::function<irods::error(irods::plugin_context&, dataObjInfo_t*, dataObjInfo_t*, keyValPair_t*)>(db_reg_replica_op));
        add_operation<dataObjInfo_t*, keyValPair_t*>(irods::DATABASE_OP_UNREG_REPLICA, std::function<irods::error(irods::plugin_context&, dataObjInfo_t*, keyValPair_t*)>(db_unreg_replica_op));
        add_operation<const char*, char**>(irods::DATABASE_OP_UPDATE_REPLICA_ACCESS_TIME, std::function<irods::error(irods::plugin_context&, const char*, char**)>(db_update_replica_access_time));

        add_operation<const std::string*, int>(irods::DATABASE_OP_UPDATE_RESC_OBJ_COUNT, std::function<irods::error(irods::plugin_context&, const std::string*, int)>(db_update_resc_obj_count));

        add_operation<collInfo_t*>(irods::DATABASE_OP_REG_COLL, std::function<irods::error(irods::plugin_context&, collInfo_t*)>(db_reg_coll_op));
        add_operation<collInfo_t*>(irods::DATABASE_OP_REG_COLL_BY_ADMIN, std::function<irods::error(irods::plugin_context&, collInfo_t*)>(db_reg_coll_op));
        add_operation<collInfo_t*>(irods::DATABASE_OP_MOD_COLL, std::function<irods::error(irods::plugin_context&, collInfo_t*)>(db_mod_coll_op));
        add_operation<collInfo_t*>(irods::DATABASE_OP_DEL_COLL, std::function<irods::error(irods::plugin_context&, collInfo_t*)>(db_del_coll_op));
        add_operation<collInfo_t*>(irods::DATABASE_OP_DEL_COLL_BY_ADMIN, std::function<irods::error(irods::plugin_context&, collInfo_t*)>(db_del_coll_op));
        add_operation<const char*, const char*>(irods::DATABASE_OP_RENAME_COLL, std::function<irods::error(irods::plugin_context&, const char*, const char*)>(db_rename_coll_op));

        add_operation<std::map<std::string, std::string>*>(irods::DATABASE_OP_REG_RESC, std::function<irods::error(irods::plugin_context&, std::map<std::string, std::string>*)>(db_reg_resc_op));
        add_operation<const char*, const char*, const char*>(irods::DATABASE_OP_MOD_RESC, std::function<irods::error(irods::plugin_context&, const char*, const char*, const char*)>(db_mod_resc_op));
        add_operation<const char*, const char*, const char*, const char*>(irods::DATABASE_OP_MOD_RESC_DATA_PATHS, std::function<irods::error(irods::plugin_context&, const char*, const char*, const char*, const char*)>(db_mod_resc_data_paths_op));
        add_operation<const char*, const char*>(irods::DATABASE_OP_MOD_RESC_FREESPACE, std::function<irods::error(irods::plugin_context&, const char*, const char*)>(db_mod_resc_freespace_op));
        add_operation<const char*, int>(irods::DATABASE_OP_DEL_RESC, std::function<irods::error(irods::plugin_context&, const char*, int)>(db_del_resc_op));
        add_operation<const char*, const char*, const char*>(irods::DATABASE_OP_ADD_CHILD_RESC, std::function<irods::error(irods::plugin_context&, const char*, const char*, const char*)>(db_add_child_resc_op));
        add_operation<const char*, const char*>(irods::DATABASE_OP_DEL_CHILD_RESC, std::function<irods::error(irods::plugin_context&, const char*, const char*)>(db_del_child_resc_op));
        add_operation<const char*, char**>(irods::DATABASE_OP_GET_HIERARCHY_FOR_RESC, std::function<irods::error(irods::plugin_context&, const char*, char**)>(db_get_hierarchy_for_resc_op));

        add_operation<userInfo_t*>(irods::DATABASE_OP_REG_USER_RE, std::function<irods::error(irods::plugin_context&, userInfo_t*)>(db_reg_user_re_op));
        add_operation<userInfo_t*>(irods::DATABASE_OP_DEL_USER_RE, std::function<irods::error(irods::plugin_context&, userInfo_t*)>(db_del_user_re_op));
        add_operation<const char*, const char*, const char*>(irods::DATABASE_OP_MOD_USER, std::function<irods::error(irods::plugin_context&, const char*, const char*, const char*)>(db_mod_user_op));
        add_operation<const char*, const char*, const char*, const char*, int*, int*>(irods::DATABASE_OP_CHECK_AUTH, std::function<irods::error(irods::plugin_context&, const char*, const char*, const char*, const char*, int*, int*)>(db_check_auth_op));
        add_operation<const char*, const char*, const char*, int*>(irods::DATABASE_OP_CHECK_AUTH_CREDENTIALS, std::function<irods::error(irods::plugin_context&, const char*, const char*, const char*, int*)>(db_check_auth_credentials_op));
        add_operation<const char*, const char*, const char*, const char*>(irods::DATABASE_OP_MOD_GROUP, std::function<irods::error(irods::plugin_context&, const char*, const char*, const char*, const char*)>(db_mod_group_op));

        add_operation<const char*, const char*, const char*, const char*, const char*, const KeyValPair*>(irods::DATABASE_OP_SET_AVU_METADATA, std::function<irods::error(irods::plugin_context&, const char*, const char*, const char*, const char*, const char*, const KeyValPair*)>(db_set_avu_metadata_op));
        add_operation<const char*, const char*, const char*, const char*, const char*, const KeyValPair*>(irods::DATABASE_OP_ADD_AVU_METADATA, std::function<irods::error(irods::plugin_context&, const char*, const char*, const char*, const char*, const char*, const KeyValPair*)>(db_add_avu_metadata_op));
        add_operation<const char*, const char*, const char*, const char*, const char*, const KeyValPair*>(irods::DATABASE_OP_DEL_AVU_METADATA, std::function<irods::error(irods::plugin_context&, const char*, const char*, const char*, const char*, const char*, const KeyValPair*)>(db_del_avu_metadata_op));
        add_operation<const char*, const char*, const char*, const char*, const char*, const char*, const char*, const char*, const KeyValPair*>(irods::DATABASE_OP_MOD_AVU_METADATA, std::function<irods::error(irods::plugin_context&, const char*, const char*, const char*, const char*, const char*, const char*, const char*, const char*, const KeyValPair*)>(db_mod_avu_metadata_op));
        add_operation<const char*, const char*, const char*, const char*>(irods::DATABASE_OP_COPY_AVU_METADATA, std::function<irods::error(irods::plugin_context&, const char*, const char*, const char*, const char*)>(db_copy_avu_metadata_op));
        
        add_operation<const char*, const char*, const char*, const char*>(irods::DATABASE_OP_REG_ZONE, std::function<irods::error(irods::plugin_context&, const char*, const char*, const char*, const char*)>(db_reg_zone_op));
        add_operation<const char*, const char*, const char*>(irods::DATABASE_OP_MOD_ZONE, std::function<irods::error(irods::plugin_context&, const char*, const char*, const char*)>(db_mod_zone_op));
        add_operation<const char*>(irods::DATABASE_OP_DEL_ZONE, std::function<irods::error(irods::plugin_context&, const char*)>(db_del_zone_op));

        add_operation<const char*, const char*, const char*>(irods::DATABASE_OP_REG_TOKEN, std::function<irods::error(irods::plugin_context&, const char*, const char*, const char*)>(db_reg_token_op));
        add_operation<const char*, const char*>(irods::DATABASE_OP_DEL_TOKEN, std::function<irods::error(irods::plugin_context&, const char*, const char*)>(db_del_token_op));

        add_operation<const char*, const char*, rodsLong_t>(irods::DATABASE_OP_SET_QUOTA, std::function<irods::error(irods::plugin_context&, const char*, const char*, rodsLong_t)>(db_set_quota_op));
        add_operation<const char*, const char*, rodsLong_t*, int*>(irods::DATABASE_OP_CHECK_QUOTA, std::function<irods::error(irods::plugin_context&, const char*, const char*, rodsLong_t*, int*)>(db_check_quota_op));
        add_operation(irods::DATABASE_OP_CALC_USAGE_AND_QUOTA, std::function<irods::error(irods::plugin_context&)>(db_calc_usage_and_quota_op));

        add_operation<ruleExecSubmitInp_t*>(irods::DATABASE_OP_REG_RULE_EXEC, std::function<irods::error(irods::plugin_context&, ruleExecSubmitInp_t*)>(db_reg_rule_exec_op));
        add_operation<const char*>(irods::DATABASE_OP_DEL_RULE_EXEC, std::function<irods::error(irods::plugin_context&, const char*)>(db_del_rule_exec_op));

        add_operation<const char*, const char*>(irods::DATABASE_OP_ADD_SPECIFIC_QUERY, std::function<irods::error(irods::plugin_context&, const char*, const char*)>(db_add_specific_query_op));
        add_operation<const char*>(irods::DATABASE_OP_DEL_SPECIFIC_QUERY, std::function<irods::error(irods::plugin_context&, const char*)>(db_del_specific_query_op));

        add_operation<const char*, rodsLong_t>(irods::DATABASE_OP_SET_LOGICAL_QUOTA, std::function<irods::error(irods::plugin_context&, const char*, rodsLong_t)>(db_set_logical_quota_op));
        add_operation<const char*, std::vector<std::tuple<std::string, std::int64_t, std::int64_t, std::int64_t, std::int64_t>>*>(irods::DATABASE_OP_CHECK_LOGICAL_QUOTA, std::function<irods::error(irods::plugin_context&, const char*, std::vector<std::tuple<std::string, std::int64_t, std::int64_t, std::int64_t, std::int64_t>>*)>(db_check_logical_quota_op));
        add_operation<const char*, rodsLong_t*, rodsLong_t*>(irods::DATABASE_OP_CALC_LOGICAL_USAGE_AND_QUOTA, std::function<irods::error(irods::plugin_context&, const char*, rodsLong_t*, rodsLong_t*)>(db_calc_logical_usage_and_quota_op));

        add_operation<const char*, int>(irods::DATABASE_OP_REG_SERVER_LOAD, std::function<irods::error(irods::plugin_context&, const char*, int)>(db_reg_server_load_op));
        add_operation<const char*>(irods::DATABASE_OP_PURGE_SERVER_LOAD, std::function<irods::error(irods::plugin_context&, const char*)>(db_purge_server_load_op));

        add_operation<const char*, const char*, const char*>(irods::DATABASE_OP_SET_GRID_CONFIGURATION_VALUE, std::function<irods::error(irods::plugin_context&, const char*, const char*, const char*)>(db_set_grid_configuration_value_op));
        add_operation<const char*, const char*, char*, std::size_t>(irods::DATABASE_OP_GET_GRID_CONFIGURATION_VALUE, std::function<irods::error(irods::plugin_context&, const char*, const char*, char*, std::size_t)>(db_get_grid_configuration_value_op));

        add_operation<int, const char*, const char*, const char*, const char*>(irods::DATABASE_OP_MOD_ACCESS_CONTROL, std::function<irods::error(irods::plugin_context&, int, const char*, const char*, const char*, const char*)>(db_mod_access_control_op));
        add_operation<rodsLong_t>(irods::DATABASE_OP_CHECK_PERMISSION_TO_MODIFY_DATA_OBJECT, std::function<irods::error(irods::plugin_context&, rodsLong_t)>(db_check_permission_to_modify_data_object_op));
        add_operation<rodsLong_t, rodsLong_t>(irods::DATABASE_OP_UPDATE_TICKET_WRITE_BYTE_COUNT, std::function<irods::error(irods::plugin_context&, rodsLong_t, rodsLong_t)>(db_update_ticket_write_byte_count_op));
        add_operation<const char*>(irods::DATABASE_OP_DATA_OBJECT_FINALIZE, std::function<irods::error(irods::plugin_context&, const char*)>(db_data_object_finalize_op));
        add_operation<const char*, std::vector<std::string>*>(irods::DATABASE_OP_GET_DELAY_RULE_INFO, std::function<irods::error(irods::plugin_context&, const char*, std::vector<std::string>*)>(db_get_delay_rule_info_op));
        add_operation<const char*, const char*>(irods::DATABASE_OP_DELAY_RULE_LOCK, std::function<irods::error(irods::plugin_context&, const char*, const char*)>(db_delay_rule_lock_op));
        add_operation<const char*, const char*>(irods::DATABASE_OP_DELAY_RULE_UNLOCK, std::function<irods::error(irods::plugin_context&, const char*, const char*)>(db_delay_rule_unlock_op));
        add_operation<const char*, const char*, const char*, int*>(irods::DATABASE_OP_CHECK_PASSWORD, std::function<irods::error(irods::plugin_context&, const char*, const char*, const char*, int*)>(db_check_password_op));
        add_operation<const char*, const char*, char*>(irods::DATABASE_OP_MAKE_SESSION_TOKEN, std::function<irods::error(irods::plugin_context&, const char*, const char*, char*)>(db_make_session_token_op));
        add_operation<const char*, const char*, const char*, int*>(irods::DATABASE_OP_CHECK_SESSION_TOKEN, std::function<irods::error(irods::plugin_context&, const char*, const char*, const char*, int*)>(db_check_session_token_op));
        add_operation<const char*, const char*>(irods::DATABASE_OP_REMOVE_SESSION_TOKENS, std::function<irods::error(irods::plugin_context&, const char*, const char*)>(db_remove_session_tokens_op));
        add_operation<const char*, const char*>(irods::DATABASE_OP_REMOVE_PASSWORD, std::function<irods::error(irods::plugin_context&, const char*, const char*)>(db_remove_password_op));

        add_operation<int*>("database_get_catalog_version", std::function<irods::error(irods::plugin_context&, int*)>(db_get_catalog_version_op));
        add_operation("database_initialize_catalog", std::function<irods::error(irods::plugin_context&)>(db_initialize_catalog_op));
        add_operation<genQueryInp_t*, genQueryOut_t*>(irods::DATABASE_OP_GEN_QUERY, std::function<irods::error(irods::plugin_context&, genQueryInp_t*, genQueryOut_t*)>(db_gen_query_op));
    }
};

extern "C" irods::database* plugin_factory(const std::string& _inst_name, const std::string& _context) { return new l3kvg_database_plugin(_inst_name, _context); }
