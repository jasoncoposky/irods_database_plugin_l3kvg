#include "irods/irods_database_plugin.hpp"
#include "irods/irods_database_constants.hpp"
#include "irods/irods_server_properties.hpp"
#include "irods/irods_configuration_keywords.hpp"
#include "irods/catalog/catalog_facade.hpp"
#include "irods/filesystem/path.hpp"
#include "irods/catalog/gq2_compiler.hpp"
#include "irods/private/genquery2_driver.hpp"
#include "irods/private/genquery2_sql.hpp"
#include "buffer.hpp"
#include "json.hpp"
#include "L3KVG/Node.hpp"
#include "irods/rodsLog.h"
#include "irods/rodsErrorTable.h"
#include "irods/irods_exception.hpp"
#include "irods/objInfo.h"
#include "irods/specificQuery.h"
#include "irods/rsGenQuery.hpp"
#include "irods/rcMisc.h"
#include "irods/obf.h"
#include "irods/rodsUser.h"
#include "irods/authenticate.h"
#include "irods/irods_random.hpp"
#include "irods/checksum.h"
#include "irods/irods_children_parser.hpp"
#include "irods/irods_resource_constants.hpp"
#include "irods/irods_rs_comm_query.hpp"

#include <memory>
#include <string>
#include <vector>
#include <map>
#include <cstring>
#include <iostream>
#include <ctime>
#include <pthread.h>
#include <unistd.h>

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

inline std::string safe_string(std::string_view sv) {
    return sanitize_utf8(sv);
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
        char buf[32];
        std::snprintf(buf, sizeof(buf), "%011lld", static_cast<long long>(std::time(nullptr)));
        return std::string(buf);
    }
    try {
        if (!ts.empty() && std::all_of(ts.begin(), ts.end(), ::isdigit)) {
            char buf[32];
            std::snprintf(buf, sizeof(buf), "%011lld", std::stoll(ts));
            return std::string(buf);
        }
    } catch (...) {}
    return ts;
}

#ifndef KW_CFG_ZONE_NAME
#define KW_CFG_ZONE_NAME "zone_name"
#endif
#ifndef KW_CFG_ZONE_USER
#define KW_CFG_ZONE_USER "zone_user"
#endif

static std::unique_ptr<irods::catalog::CatalogFacade> g_catalog;
static pid_t g_catalog_pid = 0;
static std::once_flag g_atfork_once;
static std::string g_zone_name = "tempZone";
static std::string g_zone_user = "rods";
static char g_session_ticket[NAME_LEN]{};
static char g_session_client_addr[NAME_LEN]{};
static char g_session_client_user[NAME_LEN]{};
static char g_session_client_zone[NAME_LEN]{};

static std::mutex s_rid_cache_mu;
static std::unordered_map<std::string, uint64_t> s_rid_cache;

irods::error init_l3kvg_catalog();

static void atfork_child() {
    rodsLog(LOG_DEBUG, "L3_PLUGIN: atfork_child() in PID %d, resetting catalog from parent PID %d", getpid(), g_catalog_pid);
    g_catalog.release();
    g_catalog = nullptr;
    g_catalog_pid = 0;
    g_session_ticket[0] = '\0';
    g_session_client_addr[0] = '\0';
    g_session_client_user[0] = '\0';
    g_session_client_zone[0] = '\0';
    init_l3kvg_catalog();
    if (g_catalog) {
        g_catalog->reset_ticket_session_state();
    }
}

irods::error init_l3kvg_catalog() {
    std::call_once(g_atfork_once, []() {
        pthread_atfork(nullptr, nullptr, atfork_child);
    });

    pid_t current_pid = getpid();
    if (g_catalog && g_catalog_pid == current_pid) return SUCCESS();
    if (g_catalog && g_catalog_pid != current_pid) {
        g_catalog.release();
        g_catalog = nullptr;
    }
    try {
        const auto& config_handle = irods::server_properties::instance().map();
        const auto& config_json = config_handle.get_json();
        
        if (!config_json.contains("plugin_configuration") || !config_json.at("plugin_configuration").contains("database")) {
            return ERROR(SYS_CONFIG_FILE_ERR, "Missing plugin_configuration/database");
        }
        const auto& db_config = config_json.at("plugin_configuration").at("database");
        
        lite3cpp::Buffer spec_config;
        if (db_config.contains(irods::KW_CFG_PLUGIN_SPECIFIC_CONFIGURATION)) {
            spec_config = lite3cpp::lite3_json::from_json_string(db_config.at(irods::KW_CFG_PLUGIN_SPECIFIC_CONFIGURATION).dump());
        } else if (db_config.contains("l3kvg") && db_config.at("l3kvg").contains(irods::KW_CFG_PLUGIN_SPECIFIC_CONFIGURATION)) {
            spec_config = lite3cpp::lite3_json::from_json_string(db_config.at("l3kvg").at(irods::KW_CFG_PLUGIN_SPECIFIC_CONFIGURATION).dump());
        }

        irods::catalog::Config cfg;
        cfg.db_path = (spec_config.size() >= sizeof(lite3cpp::PackedNodeLayout) && spec_config.get_type(0, "db_path") == lite3cpp::Type::String) 
                      ? std::string(spec_config.get_str(0, "db_path")) : "/var/lib/irods/l3kvg_db";
        cfg.node_id = (spec_config.size() >= sizeof(lite3cpp::PackedNodeLayout) && spec_config.get_type(0, "node_id") == lite3cpp::Type::Int64) 
                      ? static_cast<uint32_t>(spec_config.get_i64(0, "node_id")) : 0;
        cfg.zmq_endpoint = (spec_config.size() >= sizeof(lite3cpp::PackedNodeLayout) && spec_config.get_type(0, "zmq_endpoint") == lite3cpp::Type::String) 
                           ? std::string(spec_config.get_str(0, "zmq_endpoint")) : "tcp://127.0.0.1:5555";

        if (spec_config.size() >= sizeof(lite3cpp::PackedNodeLayout) && spec_config.get_type(0, "federation") == lite3cpp::Type::Array) {
            size_t fed_ofs = spec_config.get_arr(0, "federation");
            lite3cpp::NodeView fnv(reinterpret_cast<const lite3cpp::PackedNodeLayout*>(spec_config.data() + fed_ofs));
            for (uint32_t i = 0; i < fnv.size(); ++i) {
                if (spec_config.arr_get_type(fed_ofs, i) == lite3cpp::Type::Object) {
                    size_t f_item = spec_config.arr_get_obj(fed_ofs, i);
                    std::string name = (spec_config.get_type(f_item, "name") == lite3cpp::Type::String) ? std::string(spec_config.get_str(f_item, "name")) : "";
                    uint16_t fid = (spec_config.get_type(f_item, "id") == lite3cpp::Type::Int64) ? static_cast<uint16_t>(spec_config.get_i64(f_item, "id")) : 0;
                    std::string ep = (spec_config.get_type(f_item, "endpoint") == lite3cpp::Type::String) ? std::string(spec_config.get_str(f_item, "endpoint")) : "";
                    cfg.federation.push_back({std::move(name), fid, std::move(ep)});
                }
            }
        }

        const std::string& zone_name = config_json.value(KW_CFG_ZONE_NAME, "tempZone");
        g_zone_name = zone_name;
        g_zone_user = config_json.value(KW_CFG_ZONE_USER, "rods");
        cfg.default_resc = config_json.value("default_resource_name", "demoResc");
        cfg.default_resc_vault = config_json.value("default_resource_directory", "/var/lib/irods/Vault");
        cfg.admin_user = g_zone_user;
        cfg.cluster_id = irods::catalog::SnowflakeID::calculate_cluster_id(zone_name);

        l3kvg::Settings settings;
        settings.node_id = cfg.node_id;
        settings.fed_timeout_ms = config_json.value("fed_timeout_ms", 30000);

        auto new_catalog = std::make_unique<irods::catalog::CatalogFacade>();
        if (auto ret = new_catalog->init(cfg, zone_name, settings); !ret.ok()) {
            return ret;
        }

        g_catalog = std::move(new_catalog);
        g_catalog_pid = current_pid;

        return SUCCESS();
    } catch (const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: init_l3kvg_catalog EXCEPTION: %s", e.what());
        return ERROR(SYS_CONFIG_FILE_ERR, e.what());
    }
}

irods::error db_maintenance_op(irods::lookup_table<boost::any>& _props) { return init_l3kvg_catalog(); }
irods::error db_start_op(irods::plugin_context& _ctx) { return init_l3kvg_catalog(); }
irods::error db_stop_op(irods::plugin_context& _ctx) {
    if (g_catalog && g_catalog_pid == getpid()) {
        g_catalog.reset();
    } else {
        g_catalog.release();
    }
    g_catalog_pid = 0;
    return SUCCESS();
}
irods::error db_open_op(irods::plugin_context& _ctx) { return SUCCESS(); }
irods::error db_close_op(irods::plugin_context& _ctx) { return SUCCESS(); }
irods::error db_commit_op(irods::plugin_context& _ctx) { return SUCCESS(); }
irods::error db_rollback_op(irods::plugin_context& _ctx) { return SUCCESS(); }

// Data Objects
irods::error db_reg_data_obj_op(irods::plugin_context& _ctx, dataObjInfo_t* _info) {
    try {
        rodsLog(LOG_DEBUG, "L3_PLUGIN: ENTERING db_reg_data_obj_op");
        if (!_info) return ERROR(SYS_INVALID_INPUT_PARAM, "null dataObjInfo_t");
        irods::catalog::data_object obj;
        obj.id = (uint64_t)_info->dataId; 
        obj.coll_id = (uint64_t)_info->collId; 
        
        std::string full_path = safe_string(_info->objPath);
        obj.full_path = full_path;
        irods::experimental::filesystem::path p(full_path);
        obj.name = p.object_name().string();
        std::string parent_path = p.parent_path().string();

        irods::catalog::EntityType parent_type = irods::catalog::EntityType::Collection;
        irods::catalog::snowflake_id_t parent_sid = 0;
        if (!g_catalog->resolve_path(parent_path, parent_sid, parent_type).ok() || !parent_sid) {
            rodsLog(LOG_DEBUG, "L3_PLUGIN: db_reg_data_obj_op parent collection unknown: %s", parent_path.c_str());
            if (_ctx.comm()) {
                addRErrorMsg(&_ctx.comm()->rError, 0, ("collection '" + parent_path + "' is unknown").c_str());
            }
            return ERROR(CAT_UNKNOWN_COLLECTION, "parent collection unknown");
        }

        static std::mutex s_cid_cache_mu;
        static std::unordered_map<irods::catalog::snowflake_id_t, uint64_t> s_cid_cache;
        if (obj.coll_id == 0) {
            {
                std::lock_guard<std::mutex> lock(s_cid_cache_mu);
                auto it = s_cid_cache.find(parent_sid);
                if (it != s_cid_cache.end()) {
                    obj.coll_id = it->second;
                }
            }
            if (obj.coll_id == 0) {
                auto payload = g_catalog->get_client()->get_node_payload_async(g_catalog->get_cluster_id(), parent_sid).get();
                if (!payload.empty()) {
                    lite3cpp::Buffer buf(payload);
                    obj.coll_id = buf.get_i64(0, "id");
                    std::lock_guard<std::mutex> lock(s_cid_cache_mu);
                    s_cid_cache[parent_sid] = obj.coll_id;
                }
            }
        }
        _info->collId = obj.coll_id;

        bool is_admin = false;
        std::string user_name;
        if (_ctx.comm()) {
            user_name = _ctx.comm()->clientUser.userName;
            if (irods::is_privileged_client(*_ctx.comm())) {
                is_admin = true;
            }
        } else {
            const auto& config = irods::server_properties::instance().map().get_json();
            if (config.contains(KW_CFG_ZONE_USER)) {
                user_name = config.at(KW_CFG_ZONE_USER).get<std::string>();
            }
            is_admin = true;
        }

        if (!is_admin) {
            bool allowed = false;
            irods::catalog::snowflake_id_t usid = 0;
            if (g_session_ticket[0] != '\0') {
                std::string cuser = user_name.empty() ? g_session_client_user : user_name;
                auto terr = g_catalog->check_ticket_access(g_session_ticket, parent_sid, "write", cuser, g_session_client_addr);
                if (terr.ok()) {
                    allowed = true;
                } else {
                    std::string tpath, ttype;
                    auto vret = g_catalog->validate_ticket(g_session_ticket, cuser, g_session_client_addr, &tpath, &ttype);
                    if (!vret.ok()) {
                        return vret;
                    }
                    if (ttype != "write") {
                        return ERROR(CAT_NO_ACCESS_PERMISSION, "Ticket is read-only");
                    }
                    if (tpath == full_path || tpath == parent_path || full_path.starts_with(tpath == "/" ? "/" : tpath + "/")) {
                        allowed = true;
                    } else {
                        return ERROR(CAT_TICKET_INVALID, "Ticket does not apply to this path");
                    }
                }
            } else {
                if (g_catalog->resolve_user_name(user_name, usid).ok()) {
                    g_catalog->check_permission(usid, parent_sid, "write", allowed);
                }
            }
            if (!allowed) {
                rodsLog(LOG_DEBUG, "L3_PLUGIN: db_reg_data_obj_op: Access Denied for user [%s] on parent [%s]",
                        user_name.c_str(), parent_path.c_str());
                if (_ctx.comm()) {
                    addRErrorMsg(&_ctx.comm()->rError, 0,
                                 ("no permission to update collection '" + parent_path + "'").c_str());
                }
                return ERROR(CAT_NO_ACCESS_PERMISSION, "User does not have permission to register data object in collection");
            }
        }

        obj.size = (uint64_t)_info->dataSize;
        obj.owner_name = safe_string(_info->dataOwnerName); 
        if (obj.owner_name.empty()) {
            if (_ctx.comm()) {
                obj.owner_name = _ctx.comm()->clientUser.userName;
            } else {
                const auto& config = irods::server_properties::instance().map().get_json();
                if (config.contains(KW_CFG_ZONE_USER)) {
                    obj.owner_name = config.at(KW_CFG_ZONE_USER).get<std::string>();
                }
            }
        }
        obj.owner_zone = safe_string(_info->dataOwnerZone);
        if (obj.owner_zone.empty()) {
            if (_ctx.comm()) {
                obj.owner_zone = _ctx.comm()->clientUser.rodsZone;
            } else {
                const auto& config = irods::server_properties::instance().map().get_json();
                if (config.contains(KW_CFG_ZONE_NAME)) {
                    obj.owner_zone = config.at(KW_CFG_ZONE_NAME).get<std::string>();
                }
            }
        }
        if (safe_string(_info->dataExpiry).empty()) {
            strncpy(_info->dataExpiry, "00000000000", sizeof(_info->dataExpiry) - 1);
        }
        obj.expiry = safe_string(_info->dataExpiry);
        if (obj.expiry.empty()) obj.expiry = "00000000000";
        obj.mode = safe_string(_info->dataMode);
        obj.version = safe_string(_info->version);
        obj.comments = safe_string(_info->dataComments);
        obj.create_ts = get_timestamp(safe_string(_info->dataCreate)); 
        obj.modify_ts = get_timestamp(safe_string(_info->dataModify));
        obj.type = safe_string(_info->dataType);
        if (obj.type.empty()) obj.type = "generic";
        obj.checksum = safe_string(_info->chksum);
        obj.status = safe_string(_info->statusString);

        if (obj.id <= 0) g_catalog->get_next_sequence_value("R_DATA_MAIN", obj.id);

        rodsLog(LOG_DEBUG, "L3_PLUGIN: db_reg_data_obj_op: obj.name=%s, obj.coll_id=%llu, obj.id=%llu", obj.name.c_str(), (unsigned long long)obj.coll_id, (unsigned long long)obj.id);

        // Prepare initial replica
        irods::catalog::replica repl;
        repl.data_id = (uint64_t)obj.id;
        repl.replica_number = (uint32_t)_info->replNum;
        repl.resource_id = (uint64_t)_info->rescId;
        repl.physical_path = safe_string(_info->filePath);
        repl.resc_hier = safe_string(_info->rescHier);
        repl.status = std::to_string(_info->replStatus);
        repl.checksum = safe_string(_info->chksum);
        repl.modify_ts = get_timestamp(safe_string(_info->dataModify));
        repl.size = (int64_t)_info->dataSize;

        if (repl.resc_hier.empty() && _info->rescName && _info->rescName[0] != '\0') {
            repl.resc_hier = _info->rescName;
        }

        if (repl.resource_id == 0 && _info->rescName && _info->rescName[0] != '\0') {
            std::string rname(_info->rescName);
            {
                std::lock_guard<std::mutex> lock(s_rid_cache_mu);
                auto it = s_rid_cache.find(rname);
                if (it != s_rid_cache.end()) {
                    repl.resource_id = it->second;
                }
            }
            if (repl.resource_id == 0) {
                irods::catalog::snowflake_id_t rsid;
                if (g_catalog->resolve_resource_name(_info->rescName, rsid).ok()) {
                    auto payload = g_catalog->get_client()->get_node_payload_async(g_catalog->get_cluster_id(), rsid).get();
                    if (!payload.empty()) {
                        lite3cpp::Buffer buf(payload);
                        repl.resource_id = buf.get_i64(0, "id");
                        std::lock_guard<std::mutex> lock(s_rid_cache_mu);
                        s_rid_cache[rname] = repl.resource_id;
                    }
                }
            }
        }

        irods::catalog::data_id_t out_id;
        auto ret = g_catalog->register_data_object(obj, out_id, &repl);
        if (!ret.ok()) {
            rodsLog(LOG_DEBUG, "L3_PLUGIN: EXITING db_reg_data_obj_op ERROR: %ld - %s", ret.code(), ret.result().c_str());
            return ret;
        }
        _info->dataId = out_id;

        rodsLog(LOG_DEBUG, "L3_PLUGIN: EXITING db_reg_data_obj_op SUCCESS");
        return ret;
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_reg_data_obj_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

irods::error db_mod_data_obj_meta_op(irods::plugin_context& _ctx, dataObjInfo_t* _info, keyValPair_t* _reg_param) {
    try {
        rodsLog(LOG_DEBUG, "L3_PLUGIN: ENTERING db_mod_data_obj_meta_op");
        if (!_info) return ERROR(SYS_INVALID_INPUT_PARAM, "null dataObjInfo_t");

        uint64_t data_id = (uint64_t)_info->dataId;
        irods::catalog::snowflake_id_t sid = 0;
        if (data_id == 0 && _info->objPath && _info->objPath[0] != '\0') {
            irods::catalog::EntityType type;
            if (g_catalog->resolve_path(_info->objPath, sid, type).ok() && type == irods::catalog::EntityType::DataObject) {
                auto payload = g_catalog->get_client()->get_node_payload_async(g_catalog->get_cluster_id(), sid).get();
                if (!payload.empty()) {
                    lite3cpp::Buffer buf(payload);
                    try {
                        data_id = (uint64_t)buf.get_i64(0, "id");
                        _info->dataId = data_id;
                    } catch (...) {}
                }
            }
        }

        if (data_id == 0) {
            rodsLog(LOG_DEBUG, "L3_PLUGIN: db_mod_data_obj_meta_op: data object not found for path [%s]", safe_string(_info->objPath).c_str());
            return ERROR(CAT_UNKNOWN_FILE, "data object not found");
        }

        bool admin_mode = false;
        std::string user_name = _ctx.comm() ? safe_string(_ctx.comm()->clientUser.userName) : "";

        if (_reg_param && getValByKey(_reg_param, ADMIN_KW)) {
            irods::catalog::snowflake_id_t usid = 0;
            std::string pw;
            int priv = 0;
            if (g_catalog->get_user_password_and_priv(user_name, "", pw, priv).ok() && priv >= 5) {
                admin_mode = true;
            } else {
                return ERROR(CAT_INSUFFICIENT_PRIVILEGE_LEVEL, "failed with insufficient privilege");
            }
        }

        if (!admin_mode && !user_name.empty()) {
            std::string req_level = "write";
            if (_reg_param && getValByKey(_reg_param, DATA_EXPIRY_KW) != nullptr) {
                req_level = "own";
            } else if (_reg_param && _reg_param->len == 1 &&
                      (std::string(safe_string(_reg_param->keyWord[0])) == CHKSUM_KW ||
                       std::string(safe_string(_reg_param->keyWord[0])) == "chksum")) {
                req_level = "read";
            }

            bool allowed = false;
            irods::catalog::snowflake_id_t usid = 0;
            irods::catalog::snowflake_id_t dsid = sid ? sid : g_catalog->make_id(irods::catalog::EntityType::DataObject, data_id);
            if (g_session_ticket[0] != '\0') {
                std::string cuser = user_name.empty() ? g_session_client_user : user_name;
                auto terr = g_catalog->check_ticket_access(g_session_ticket, dsid, req_level, cuser, g_session_client_addr);
                if (!terr.ok()) {
                    return terr;
                }
                allowed = true;
            } else {
                if (g_catalog->resolve_user_name(user_name, usid).ok()) {
                    g_catalog->check_permission(usid, dsid, req_level, allowed);
                }
            }
            rodsLog(LOG_DEBUG, "L3_PLUGIN: db_mod_data_obj_meta_op: user=%s usid=%llx dsid=%llx level=%s allowed=%d",
                    user_name.c_str(), (unsigned long long)usid, (unsigned long long)dsid, req_level.c_str(), allowed ? 1 : 0);
            if (!allowed) {
                rodsLog(LOG_DEBUG, "L3_PLUGIN: db_mod_data_obj_meta_op: Access Denied for user [%s] on object [%llu] with level [%s]",
                        user_name.c_str(), (unsigned long long)data_id, req_level.c_str());
                return ERROR(CAT_NO_ACCESS_PERMISSION, "User does not have permission to modify data object metadata");
            }
        }

        bool all_repl_status = false;
        std::vector<std::pair<std::string, std::string>> updates;
        if (_reg_param) {
            for (int i = 0; i < _reg_param->len; ++i) {
                std::string kw = safe_string(_reg_param->keyWord[i]);
                std::string val = safe_string(_reg_param->value[i]);
                if (kw == DATA_MODIFY_KW || kw == "dataModify" || kw == DATA_CREATE_KW || kw == DATA_EXPIRY_KW) {
                    val = get_timestamp(val);
                }
                if (kw == ALL_REPL_STATUS_KW) {
                    all_repl_status = true;
                }
                updates.emplace_back(kw, val);
                if (data_id > 0) {
                    g_catalog->modify_data_object(data_id, kw, val);
                }
            }
        }

        if (_info->dataSize > 0 && data_id > 0) {
            bool has_size = false;
            for (const auto& [k, v] : updates) {
                if (k == "dataSize" || k == DATA_SIZE_KW) { has_size = true; break; }
            }
            if (!has_size) {
                g_catalog->modify_data_object(data_id, "dataSize", std::to_string(_info->dataSize));
                updates.emplace_back("dataSize", std::to_string(_info->dataSize));
            }
        }

        if (data_id > 0) {
            uint32_t target_repl_num = (uint32_t)_info->replNum;
            if (_reg_param) {
                if (const char* rn_val = getValByKey(_reg_param, REPL_NUM_KW)) {
                    try { target_repl_num = static_cast<uint32_t>(std::stoul(rn_val)); } catch (...) {}
                }
            }

            std::string target_resc_hier = safe_string(_info->rescHier);
            if (target_resc_hier.empty() && _reg_param) {
                if (const char* rh_val = getValByKey(_reg_param, RESC_HIER_STR_KW)) {
                    target_resc_hier = rh_val;
                }
            }
            if (target_resc_hier.empty() && _info->rescName && _info->rescName[0] != '\0') {
                target_resc_hier = _info->rescName;
            }
            if (target_resc_hier.empty() && _reg_param) {
                if (const char* rn_val = getValByKey(_reg_param, RESC_NAME_KW)) {
                    target_resc_hier = rn_val;
                }
            }

            bool all_replicas = (_reg_param && getValByKey(_reg_param, ALL_KW) != nullptr);

            rodsLog(LOG_DEBUG, "L3_PLUGIN: db_mod_data_obj_meta_op calling modify_replicas_for_data_object data_id=%llu target_repl_num=%u target_resc_hier='%s' all_replicas=%d updates_len=%zu",
                    (unsigned long long)data_id, target_repl_num, target_resc_hier.c_str(), all_replicas ? 1 : 0, updates.size());

            g_catalog->modify_replicas_for_data_object(data_id, target_repl_num, target_resc_hier, updates, all_repl_status, all_replicas);
        }

        rodsLog(LOG_DEBUG, "L3_PLUGIN: EXITING db_mod_data_obj_meta_op SUCCESS");
        return SUCCESS();
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_mod_data_obj_meta_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

irods::error db_rename_object_op(irods::plugin_context& _ctx, rodsLong_t _obj_id, const char* _new_name) {
    try {
        rodsLog(LOG_DEBUG, "L3_PLUGIN: ENTERING db_rename_object_op");
        auto ret = g_catalog->rename_object((uint64_t)_obj_id, safe_string(_new_name));
        rodsLog(LOG_DEBUG, "L3_PLUGIN: EXITING db_rename_object_op SUCCESS");
        return ret;
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_rename_object_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

irods::error db_move_object_op(irods::plugin_context& _ctx, rodsLong_t _obj_id, rodsLong_t _target_coll_id) {
    try {
        rodsLog(LOG_DEBUG, "L3_PLUGIN: ENTERING db_move_object_op");
        auto ret = g_catalog->move_object((uint64_t)_obj_id, (uint64_t)_target_coll_id);
        rodsLog(LOG_DEBUG, "L3_PLUGIN: EXITING db_move_object_op SUCCESS");
        return ret;
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_move_object_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

// Replicas
irods::error db_reg_replica_op(irods::plugin_context& _ctx, dataObjInfo_t* _src, dataObjInfo_t* _dst, keyValPair_t* _cond) {
    try {
        rodsLog(LOG_DEBUG, "L3_PLUGIN: ENTERING db_reg_replica_op");
        if (!_dst) return ERROR(SYS_INVALID_INPUT_PARAM, "null dataObjInfo_t (dst)");

        uint64_t data_id = _dst->dataId > 0 ? (uint64_t)_dst->dataId : (_src ? (uint64_t)_src->dataId : 0);
        uint32_t next_rn = (_dst->replNum >= 0 && _dst->dataId == 0) ? (uint32_t)_dst->replNum : g_catalog->get_next_replica_number(data_id);
        _dst->replNum = next_rn;
        if (_dst->dataId == 0 && data_id > 0) {
            _dst->dataId = data_id;
        }

        std::string status_str = std::to_string(_dst->replStatus);
        if (_cond && getValByKey(_cond, REGISTER_AS_INTERMEDIATE_KW)) {
            status_str = "2"; // intermediate
        } else if (_dst->replStatus == 0 && _src && _src->replStatus > 0) {
            status_str = std::to_string(_src->replStatus);
        }

        int64_t size = (int64_t)_dst->dataSize;
        if (size <= 0 && _src && _src->dataSize > 0) {
            size = _src->dataSize;
            _dst->dataSize = _src->dataSize;
        }

        std::string chksum = safe_string(_dst->chksum);
        if (chksum.empty() && _src && _src->chksum[0] != '\0') {
            chksum = safe_string(_src->chksum);
            rstrcpy(_dst->chksum, _src->chksum, NAME_LEN);
        }

        std::string resc_hier = safe_string(_dst->rescHier);
        if (resc_hier.empty() && _dst->rescName[0] != '\0') {
            resc_hier = _dst->rescName;
            rstrcpy(_dst->rescHier, _dst->rescName, MAX_NAME_LEN);
        }

        uint64_t resc_id = (uint64_t)_dst->rescId;
        if (resc_id == 0 && _dst->rescName[0] != '\0') {
            std::string rname(_dst->rescName);
            {
                std::lock_guard<std::mutex> lock(s_rid_cache_mu);
                auto it = s_rid_cache.find(rname);
                if (it != s_rid_cache.end()) {
                    resc_id = it->second;
                    _dst->rescId = resc_id;
                }
            }
            if (resc_id == 0) {
                irods::catalog::snowflake_id_t rsid;
                if (g_catalog->resolve_resource_name(_dst->rescName, rsid).ok()) {
                    auto payload = g_catalog->get_client()->get_node_payload_async(g_catalog->get_cluster_id(), rsid).get();
                    if (!payload.empty()) {
                        lite3cpp::Buffer buf(payload);
                        resc_id = buf.get_i64(0, "id");
                        _dst->rescId = resc_id;
                        std::lock_guard<std::mutex> lock(s_rid_cache_mu);
                        s_rid_cache[rname] = resc_id;
                    }
                }
            }
        }

        irods::catalog::replica repl{
            data_id, 
            next_rn, 
            resc_id, 
            safe_string(_dst->filePath), 
            resc_hier, 
            status_str, 
            chksum, 
            get_timestamp(safe_string(_dst->dataModify)), 
            "",
            size};
        
        auto ret = g_catalog->register_replica(repl);
        rodsLog(LOG_DEBUG, "L3_PLUGIN: EXITING db_reg_replica_op SUCCESS");
        return ret;
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_reg_replica_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

irods::error db_unreg_replica_op(irods::plugin_context& _ctx, dataObjInfo_t* _info, keyValPair_t* _cond) {
    try {
        rodsLog(LOG_DEBUG, "L3_PLUGIN: ENTERING db_unreg_replica_op");
        if (!_info) return ERROR(SYS_INVALID_INPUT_PARAM, "null dataObjInfo_t");

        int adminMode = 0;
        int trashMode = 0;
        if (_cond != nullptr) {
            char* theVal = getValByKey(_cond, ADMIN_KW);
            if (theVal != nullptr) {
                adminMode = 1;
            }
            theVal = getValByKey(_cond, ADMIN_RMTRASH_KW);
            if (theVal != nullptr) {
                adminMode = 1;
                trashMode = 1;
            }
        }

        if (adminMode == 0 && _ctx.comm() && !irods::is_privileged_client(*_ctx.comm())) {
            std::string user_name = safe_string(_ctx.comm()->clientUser.userName);
            irods::catalog::snowflake_id_t usid = 0;
            if (!user_name.empty()) {
                g_catalog->resolve_user_name(user_name, usid);
            }

            irods::catalog::snowflake_id_t dsid = 0;
            if (_info->dataId > 0) {
                dsid = g_catalog->make_id(irods::catalog::EntityType::DataObject, (uint64_t)_info->dataId);
            } else if (_info->objPath[0] != '\0') {
                irods::catalog::EntityType out_type;
                g_catalog->resolve_path(_info->objPath, dsid, out_type);
            }

            if (dsid == 0) {
                return ERROR(CAT_UNKNOWN_FILE, "data object unknown");
            }

            bool allowed = false;
            auto perm_ret = g_catalog->check_permission(usid, dsid, "delete_object", allowed, /*check_parents=*/false);
            if (perm_ret.code() == CAT_UNKNOWN_FILE) {
                return ERROR(CAT_UNKNOWN_FILE, "data object unknown");
            }
            if (!perm_ret.ok() || !allowed) {
                rodsLog(LOG_DEBUG, "L3_PLUGIN: db_unreg_replica_op: permission denied for user [%s] on data_id [%lld]",
                        user_name.c_str(), (long long)_info->dataId);
                return ERROR(CAT_NO_ACCESS_PERMISSION, "check_data_object_only failed");
            }
        } else if (adminMode != 0) {
            if (_ctx.comm() && _ctx.comm()->clientUser.authInfo.authFlag != LOCAL_PRIV_USER_AUTH) {
                return ERROR(CAT_INSUFFICIENT_PRIVILEGE_LEVEL, "insufficient privilege");
            }
            if (trashMode) {
                std::string zone = _ctx.comm() ? safe_string(_ctx.comm()->clientUser.rodsZone) : "";
                std::string checkPath = "/" + zone + "/trash";
                std::string path = safe_string(_info->objPath);
                if (path.compare(0, checkPath.length(), checkPath) != 0) {
                    if (_ctx.comm()) {
                        addRErrorMsg(&_ctx.comm()->rError, 0, "TRASH_KW but not zone/trash path");
                    }
                    return ERROR(CAT_INVALID_ARGUMENT, "TRASH_KW but not zone/trash path");
                }
            }
        }

        auto ret = g_catalog->unregister_replica((uint64_t)_info->dataId, (uint32_t)_info->replNum);
        rodsLog(LOG_DEBUG, "L3_PLUGIN: EXITING db_unreg_replica_op SUCCESS");
        return ret;
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_unreg_replica_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

irods::error db_update_replica_access_time(irods::plugin_context& _ctx, const char* _json_input, char** _out) {
    try {
        rodsLog(LOG_DEBUG, "L3_PLUGIN: ENTERING db_update_replica_access_time");
        if (!_json_input || !_out) {
            return ERROR(SYS_INTERNAL_NULL_INPUT_ERR, "Received one or more null pointers.");
        }
        
        auto json_buf = lite3cpp::lite3_json::from_json_string(_json_input);
        if (json_buf.size() >= sizeof(lite3cpp::PackedNodeLayout) && json_buf.get_type(0, "access_time_updates") == lite3cpp::Type::Array) {
            size_t updates_ofs = json_buf.get_arr(0, "access_time_updates");
            lite3cpp::NodeView unv(reinterpret_cast<const lite3cpp::PackedNodeLayout*>(json_buf.data() + updates_ofs));
            for (uint32_t i = 0; i < unv.size(); ++i) {
                if (json_buf.arr_get_type(updates_ofs, i) != lite3cpp::Type::Object) continue;
                size_t u_ofs = json_buf.arr_get_obj(updates_ofs, i);
                uint64_t data_id = 0;
                if (json_buf.get_type(u_ofs, "data_id") == lite3cpp::Type::Int64) data_id = static_cast<uint64_t>(json_buf.get_i64(u_ofs, "data_id"));
                else if (json_buf.get_type(u_ofs, "data_id") == lite3cpp::Type::String) data_id = std::stoull(std::string(json_buf.get_str(u_ofs, "data_id")));

                uint32_t repl_num = 0;
                if (json_buf.get_type(u_ofs, "replica_number") == lite3cpp::Type::Int64) repl_num = static_cast<uint32_t>(json_buf.get_i64(u_ofs, "replica_number"));
                else if (json_buf.get_type(u_ofs, "replica_number") == lite3cpp::Type::String) repl_num = std::stoul(std::string(json_buf.get_str(u_ofs, "replica_number")));

                std::string atime = (json_buf.get_type(u_ofs, "atime") == lite3cpp::Type::String) ? std::string(json_buf.get_str(u_ofs, "atime")) : "";
                
                auto ret = g_catalog->update_replica_access_time(data_id, repl_num, atime);
                if (!ret.ok()) return ret;
            }
        }
        
        rodsLog(LOG_DEBUG, "L3_PLUGIN: EXITING db_update_replica_access_time SUCCESS");
        return SUCCESS();
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_update_replica_access_time EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

// Collections
irods::error db_reg_coll_op(irods::plugin_context& _ctx, collInfo_t* _info) {
    try {
        rodsLog(LOG_DEBUG, "L3_PLUGIN: ENTERING db_reg_coll_op name [%s] owner [%s]", _info && _info->collName ? _info->collName : "null", _info && _info->collOwnerName ? _info->collOwnerName : "null");
        if (!_info) return ERROR(SYS_INVALID_INPUT_PARAM, "null collInfo_t");
        irods::catalog::collection coll;
        coll.id = (uint64_t)_info->collId; 
        coll.name = safe_string(_info->collName); 
        coll.parent_name = safe_string(_info->collParentName);
        coll.owner_name = safe_string(_info->collOwnerName);
        coll.type = safe_string(_info->collType);
        coll.info1 = safe_string(_info->collInfo1);
        coll.info2 = safe_string(_info->collInfo2);
        coll.inheritance = safe_string(_info->collInheritance);

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
            if (_ctx.comm()) {
                coll.owner_name = _ctx.comm()->clientUser.userName;
            } else {
                const auto& config = irods::server_properties::instance().map().get_json();
                if (config.contains(KW_CFG_ZONE_USER)) {
                    coll.owner_name = config.at(KW_CFG_ZONE_USER).get<std::string>();
                }
            }
        }
        coll.owner_zone = safe_string(_info->collOwnerZone);
        if (coll.owner_zone.empty()) {
            if (_ctx.comm()) {
                coll.owner_zone = _ctx.comm()->clientUser.rodsZone;
            } else {
                const auto& config = irods::server_properties::instance().map().get_json();
                if (config.contains(KW_CFG_ZONE_NAME)) {
                    coll.owner_zone = config.at(KW_CFG_ZONE_NAME).get<std::string>();
                }
            }
        }

        // Ensure trailing slash is removed if any (unless just "/")
        if (coll.name.size() > 1 && coll.name.back() == '/') {
            coll.name.pop_back();
        }
        if (coll.parent_name.empty() && coll.name != "/") {
            size_t last_slash = coll.name.find_last_of('/');
            if (last_slash != std::string::npos) {
                coll.parent_name = (last_slash == 0) ? "/" : coll.name.substr(0, last_slash);
            }
        }

        if (coll.parent_id == 0 && !coll.parent_name.empty()) {
            irods::catalog::EntityType type;
            irods::catalog::snowflake_id_t sid;
            if (g_catalog->resolve_path(coll.parent_name, sid, type).ok()) {
                // Fetch the node to get the sequential ID
                auto payload = g_catalog->get_client()->get_node_payload_async(g_catalog->get_cluster_id(), sid).get();
                if (!payload.empty()) {
                    lite3cpp::Buffer buf(payload);
                    coll.parent_id = buf.get_i64(0, "id");
                }
            }
        }
        
        // Inspect condInput for special collection keywords
        for (int i = 0; i < _info->condInput.len; ++i) {
            std::string kw = safe_string(_info->condInput.keyWord[i]);
            std::string val = safe_string(_info->condInput.value[i]);
            if (kw == "collectionType" || kw == "coll_type" || kw == "collType") {
                coll.type = val;
            } else if (kw == "collectionInfo1" || kw == "coll_info1" || kw == "collInfo1") {
                coll.info1 = val;
            } else if (kw == "collectionInfo2" || kw == "coll_info2" || kw == "collInfo2") {
                coll.info2 = val;
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
        if (ret.ok()) {
            _info->collId = out_id;
            rodsLog(LOG_DEBUG, "L3_PLUGIN: EXITING db_reg_coll_op SUCCESS");
        } else {
            rodsLog(LOG_DEBUG, "L3_PLUGIN: EXITING db_reg_coll_op ERROR: %ld - %s", ret.code(), ret.result().c_str());
        }
        return ret;
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_reg_coll_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

irods::error db_reg_coll_by_admin_op(irods::plugin_context& _ctx, collInfo_t* _info) {
    try {
        rodsLog(LOG_DEBUG, "L3_PLUGIN: ENTERING db_reg_coll_by_admin_op");
        if (!_info) return ERROR(SYS_INVALID_INPUT_PARAM, "null collInfo_t");

        std::string coll_name = safe_string(_info->collName);
        if (coll_name.size() > 1 && coll_name.back() == '/') {
            coll_name.pop_back();
        }
        irods::catalog::snowflake_id_t sid = 0;
        irods::catalog::EntityType type;
        if (g_catalog->resolve_path(coll_name, sid, type).ok() && type == irods::catalog::EntityType::Collection) {
            auto payload = g_catalog->get_client()->get_node_payload_async(g_catalog->get_cluster_id(), sid).get();
            if (!payload.empty()) {
                try {
                    lite3cpp::Buffer buf(payload);
                    _info->collId = buf.get_i64(0, "id");
                    rodsLog(LOG_DEBUG, "L3_PLUGIN: db_reg_coll_by_admin_op: collection [%s] already exists (id %d), returning SUCCESS", coll_name.c_str(), _info->collId);
                    return SUCCESS();
                } catch (...) {}
            }
        }
        return db_reg_coll_op(_ctx, _info);
    } catch (const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_reg_coll_by_admin_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

irods::error db_mod_coll_op(irods::plugin_context& _ctx, collInfo_t* _info) {
    try {
        rodsLog(LOG_DEBUG, "L3_PLUGIN: ENTERING db_mod_coll_op");
        if (!_info) return ERROR(SYS_INVALID_INPUT_PARAM, "null collInfo_t");
        
        uint64_t coll_id = (uint64_t)_info->collId;
        if (coll_id == 0 && _info->collName && strlen(_info->collName) > 0) {
            std::string coll_name = _info->collName;
            if (coll_name.size() > 1 && coll_name.back() == '/') {
                coll_name.pop_back();
            }
            irods::catalog::snowflake_id_t sid = 0;
            irods::catalog::EntityType type;
            if (g_catalog->resolve_path(coll_name, sid, type).ok()) {
                auto payload = g_catalog->get_client()->get_node_payload_async(g_catalog->get_cluster_id(), sid).get();
                if (!payload.empty()) {
                    try {
                        lite3cpp::Buffer buf(payload);
                        coll_id = buf.get_i64(0, "id");
                    } catch (...) {}
                }
                if (coll_id == 0) coll_id = sid;
            }
        }

        if (coll_id == 0) {
            rodsLog(LOG_ERROR, "L3_PLUGIN: db_mod_coll_op: No collection ID or name provided");
            return ERROR(SYS_INVALID_INPUT_PARAM, "No collection ID or name provided");
        }

        if (_info->collType && strlen(_info->collType) > 0) {
            std::string val = strcmp(_info->collType, "NULL_SPECIAL_VALUE") == 0 ? "" : _info->collType;
            auto err = g_catalog->modify_collection(coll_id, "collectionType", val);
            if (!err.ok()) rodsLog(LOG_ERROR, "L3_PLUGIN: db_mod_coll_op modify_collection collectionType failed: %s", err.result().c_str());
        }
        if (_info->collInfo1 && strlen(_info->collInfo1) > 0) {
            std::string val = strcmp(_info->collInfo1, "NULL_SPECIAL_VALUE") == 0 ? "" : _info->collInfo1;
            auto err = g_catalog->modify_collection(coll_id, "collectionInfo1", val);
            if (!err.ok()) rodsLog(LOG_ERROR, "L3_PLUGIN: db_mod_coll_op modify_collection collectionInfo1 failed: %s", err.result().c_str());
        }
        if (_info->collInfo2 && strlen(_info->collInfo2) > 0) {
            std::string val = strcmp(_info->collInfo2, "NULL_SPECIAL_VALUE") == 0 ? "" : _info->collInfo2;
            auto err = g_catalog->modify_collection(coll_id, "collectionInfo2", val);
            if (!err.ok()) rodsLog(LOG_ERROR, "L3_PLUGIN: db_mod_coll_op modify_collection collectionInfo2 failed: %s", err.result().c_str());
        }
        if (_info->collModify && strlen(_info->collModify) > 0) {
            auto err = g_catalog->modify_collection(coll_id, "collectionMtime", _info->collModify);
            if (!err.ok()) rodsLog(LOG_ERROR, "L3_PLUGIN: db_mod_coll_op modify_collection collectionMtime failed: %s", err.result().c_str());
        }
        if (_info->collComments && strlen(_info->collComments) > 0) {
            std::string val = strcmp(_info->collComments, "NULL_SPECIAL_VALUE") == 0 ? "" : _info->collComments;
            auto err = g_catalog->modify_collection(coll_id, "collComments", val);
            if (!err.ok()) rodsLog(LOG_ERROR, "L3_PLUGIN: db_mod_coll_op modify_collection collComments failed: %s", err.result().c_str());
        }

        for (int i = 0; i < _info->condInput.len; ++i) {
            auto err = g_catalog->modify_collection(coll_id, 
                safe_string(_info->condInput.keyWord[i]), 
                safe_string(_info->condInput.value[i]));
            if (!err.ok()) rodsLog(LOG_ERROR, "L3_PLUGIN: db_mod_coll_op modify_collection keyword [%s] failed: %s", _info->condInput.keyWord[i], err.result().c_str());
        }
        rodsLog(LOG_DEBUG, "L3_PLUGIN: EXITING db_mod_coll_op SUCCESS");
        return SUCCESS();
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_mod_coll_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

irods::error db_del_coll_op(irods::plugin_context& _ctx, collInfo_t* _info) {
    try {
        rodsLog(LOG_DEBUG, "L3_PLUGIN: ENTERING db_del_coll_op");
        if (!_info) return ERROR(SYS_INVALID_INPUT_PARAM, "null collInfo_t");
        
        uint64_t coll_id = (uint64_t)_info->collId;
        std::string coll_name = _info->collName ? safe_string(_info->collName) : "";
        irods::catalog::snowflake_id_t sid = 0;

        if (!coll_name.empty()) {
            irods::catalog::snowflake_id_t resolved_sid = 0;
            irods::catalog::EntityType type;
            if (g_catalog->resolve_path(coll_name, resolved_sid, type).ok()) {
                sid = resolved_sid;
                auto payload = g_catalog->get_client()->get_node_payload_async(g_catalog->get_cluster_id(), sid).get();
                if (!payload.empty()) {
                    try {
                        lite3cpp::Buffer buf(payload);
                        coll_id = buf.get_i64(0, "id");
                    } catch (...) {}
                }
            }
        }

        if (sid == 0 && coll_id != 0) {
            sid = g_catalog->make_id(irods::catalog::EntityType::Collection, coll_id);
            std::string payload = g_catalog->get_client()->get_node_payload_async(g_catalog->get_cluster_id(), sid).get();
            if (payload.empty()) {
                std::string direct_payload = g_catalog->get_client()->get_node_payload_async(g_catalog->get_cluster_id(), coll_id).get();
                if (!direct_payload.empty()) {
                    sid = coll_id;
                    payload = direct_payload;
                }
            }
            if (!payload.empty() && coll_name.empty()) {
                try {
                    lite3cpp::Buffer buf(payload);
                    coll_name = safe_string(buf.get_str(0, "n"));
                } catch (...) {}
            }
        }

        if (sid == 0 && coll_id == 0) {
            rodsLog(LOG_DEBUG, "L3_PLUGIN: db_del_coll_op: Collection [%s] not found", coll_name.c_str());
            if (_ctx.comm()) {
                addRErrorMsg(&_ctx.comm()->rError, 0, ("collection '" + coll_name + "' is unknown").c_str());
            }
            return ERROR(CAT_UNKNOWN_COLLECTION, "Collection not found");
        }

        if (_ctx.comm()) {
            std::string user_name = safe_string(_ctx.comm()->clientUser.userName);
            irods::catalog::snowflake_id_t usid = 0;
            if (!user_name.empty()) {
                g_catalog->resolve_user_name(user_name, usid);
            }

            // Check parent collection write permission if parent collection exists
            if (!coll_name.empty()) {
                char logicalEndName[MAX_NAME_LEN];
                char logicalParentDirName[MAX_NAME_LEN];
                if (splitPathByKey(coll_name.c_str(), logicalParentDirName, MAX_NAME_LEN, logicalEndName, MAX_NAME_LEN, '/') >= 0) {
                    if (strlen(logicalParentDirName) == 0) {
                        snprintf(logicalParentDirName, sizeof(logicalParentDirName), "/");
                    }
                    irods::catalog::snowflake_id_t psid = 0;
                    irods::catalog::EntityType ptype;
                    if (g_catalog->resolve_path(logicalParentDirName, psid, ptype).ok()) {
                        bool parent_allowed = false;
                        auto p_ret = g_catalog->check_permission(usid, psid, "modify_object", parent_allowed, /*check_parents=*/false);
                        if (!p_ret.ok() || !parent_allowed) {
                            rodsLog(LOG_DEBUG, "L3_PLUGIN: db_del_coll_op: parent permission denied for user [%s] on [%s]",
                                    user_name.c_str(), logicalParentDirName);
                            return ERROR(CAT_NO_ACCESS_PERMISSION, "check_parent_collection_access failed");
                        }
                    }
                }
            }

            // Check collection delete permission
            bool allowed = false;
            auto perm_ret = g_catalog->check_permission(usid, sid ? sid : coll_id, "delete_object", allowed, /*check_parents=*/false);
            if (perm_ret.code() == CAT_UNKNOWN_FILE) {
                return ERROR(CAT_UNKNOWN_COLLECTION, "Collection not found");
            }
            if (!perm_ret.ok() || !allowed) {
                rodsLog(LOG_DEBUG, "L3_PLUGIN: db_del_coll_op: permission denied for user [%s] on collection [%s]",
                        user_name.c_str(), coll_name.c_str());
                return ERROR(CAT_NO_ACCESS_PERMISSION, "check_collection_access failed");
            }
        }

        // Check emptiness
        bool is_empty = true;
        auto empty_err = g_catalog->is_collection_empty(sid ? sid : coll_id, is_empty);
        if (!empty_err.ok()) {
            return empty_err;
        }
        if (!is_empty) {
            rodsLog(LOG_DEBUG, "L3_PLUGIN: db_del_coll_op: collection [%s] is not empty", coll_name.c_str());
            if (_ctx.comm()) {
                addRErrorMsg(&_ctx.comm()->rError, 0, ("collection '" + coll_name + "' is not empty").c_str());
            }
            return ERROR(CAT_COLLECTION_NOT_EMPTY, "Collection is not empty");
        }

        auto ret = g_catalog->delete_collection(coll_id ? coll_id : sid);
        rodsLog(LOG_DEBUG, "L3_PLUGIN: EXITING db_del_coll_op SUCCESS");
        return ret;
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_del_coll_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

irods::error db_del_coll_by_admin_op(irods::plugin_context& _ctx, collInfo_t* _info) {
    try {
        rodsLog(LOG_DEBUG, "L3_PLUGIN: ENTERING db_del_coll_by_admin_op");
        irods::error ret_ctx = _ctx.valid();
        if (!ret_ctx.ok()) return PASS(ret_ctx);
        if (!_info) return ERROR(CAT_INVALID_ARGUMENT, "null parameter");

        if (_ctx.comm() && _ctx.comm()->clientUser.authInfo.authFlag < LOCAL_PRIV_USER_AUTH) {
            return ERROR(CAT_INSUFFICIENT_PRIVILEGE_LEVEL, "insufficient privilege");
        }
        if (_ctx.comm() && _ctx.comm()->proxyUser.authInfo.authFlag < LOCAL_PRIV_USER_AUTH) {
            return ERROR(CAT_INSUFFICIENT_PRIVILEGE_LEVEL, "insufficient privilege");
        }

        uint64_t coll_id = (uint64_t)_info->collId;
        std::string coll_name = _info->collName ? safe_string(_info->collName) : "";
        irods::catalog::snowflake_id_t sid = 0;

        if (!coll_name.empty()) {
            irods::catalog::snowflake_id_t resolved_sid = 0;
            irods::catalog::EntityType type;
            if (g_catalog->resolve_path(coll_name, resolved_sid, type).ok()) {
                sid = resolved_sid;
                auto payload = g_catalog->get_client()->get_node_payload_async(g_catalog->get_cluster_id(), sid).get();
                if (!payload.empty()) {
                    try {
                        lite3cpp::Buffer buf(payload);
                        coll_id = buf.get_i64(0, "id");
                    } catch (...) {}
                }
            }
        }

        if (sid == 0 && coll_id != 0) {
            sid = g_catalog->make_id(irods::catalog::EntityType::Collection, coll_id);
            std::string payload = g_catalog->get_client()->get_node_payload_async(g_catalog->get_cluster_id(), sid).get();
            if (payload.empty()) {
                std::string direct_payload = g_catalog->get_client()->get_node_payload_async(g_catalog->get_cluster_id(), coll_id).get();
                if (!direct_payload.empty()) {
                    sid = coll_id;
                    payload = direct_payload;
                }
            }
            if (!payload.empty() && coll_name.empty()) {
                try {
                    lite3cpp::Buffer buf(payload);
                    coll_name = safe_string(buf.get_str(0, "n"));
                } catch (...) {}
            }
        }

        if (sid == 0 && coll_id == 0) {
            rodsLog(LOG_DEBUG, "L3_PLUGIN: db_del_coll_by_admin_op: Collection [%s] not found", coll_name.c_str());
            if (_ctx.comm()) {
                addRErrorMsg(&_ctx.comm()->rError, 0, ("collection '" + coll_name + "' is unknown").c_str());
            }
            return ERROR(CAT_UNKNOWN_COLLECTION, "unknown collection");
        }

        // Check emptiness
        bool is_empty = true;
        auto empty_err = g_catalog->is_collection_empty(sid ? sid : coll_id, is_empty);
        if (!empty_err.ok()) {
            return empty_err;
        }
        if (!is_empty) {
            rodsLog(LOG_DEBUG, "L3_PLUGIN: db_del_coll_by_admin_op: collection [%s] is not empty", coll_name.c_str());
            if (_ctx.comm()) {
                addRErrorMsg(&_ctx.comm()->rError, 0, ("collection '" + coll_name + "' is not empty").c_str());
            }
            return ERROR(CAT_COLLECTION_NOT_EMPTY, "collection not empty");
        }

        auto ret = g_catalog->delete_collection(coll_id ? coll_id : sid);
        rodsLog(LOG_DEBUG, "L3_PLUGIN: EXITING db_del_coll_by_admin_op SUCCESS");
        return ret;
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_del_coll_by_admin_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

irods::error db_rename_coll_op(irods::plugin_context& _ctx, const char* _old_name, const char* _new_name) {
    try {
        rodsLog(LOG_DEBUG, "L3_PLUGIN: ENTERING db_rename_coll_op");
        auto ret = g_catalog->rename_collection(safe_string(_old_name), safe_string(_new_name));
        rodsLog(LOG_DEBUG, "L3_PLUGIN: EXITING db_rename_coll_op SUCCESS");
        return ret;
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_rename_coll_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

// Resources
irods::error db_reg_resc_op(irods::plugin_context& _ctx, std::map<std::string, std::string>* _info) {
    try {
        rodsLog(LOG_DEBUG, "L3_PLUGIN: ENTERING db_reg_resc_op");
        if (auto ret = init_l3kvg_catalog(); !ret.ok()) return ret;
        irods::catalog::resource resc;
        if (_info) {
            for (auto const& [key, val] : *_info) {
                rodsLog(LOG_DEBUG, "L3_PLUGIN: db_reg_resc_op: key=[%s] val=[%s]", key.c_str(), val.c_str());
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
        rodsLog(LOG_DEBUG, "L3_PLUGIN: EXITING db_reg_resc_op SUCCESS");
        return ret;
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_reg_resc_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

irods::error db_mod_resc_op(irods::plugin_context& _ctx, const char* _resc, const char* _prop, const char* _val) {
    try {
        rodsLog(LOG_DEBUG, "L3_PLUGIN: ENTERING db_mod_resc_op");
        if (_resc) {
            std::lock_guard<std::mutex> lock(s_rid_cache_mu);
            s_rid_cache.erase(safe_string(_resc));
        }
        irods::catalog::resc_id_t rid = 0;
        if (_resc && g_catalog->resolve_resource_name(_resc, rid).ok()) {
            auto ret = g_catalog->modify_resource(rid, safe_string(_prop), safe_string(_val));
            rodsLog(LOG_DEBUG, "L3_PLUGIN: EXITING db_mod_resc_op SUCCESS");
            return ret;
        }
        rodsLog(LOG_DEBUG, "L3_PLUGIN: EXITING db_mod_resc_op SUCCESS (no resc)");
        return SUCCESS();
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_mod_resc_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

irods::error db_del_resc_op(irods::plugin_context& _ctx, const char* _resc, int _unused) {
    try {
        rodsLog(LOG_DEBUG, "L3_PLUGIN: ENTERING db_del_resc_op");
        if (_resc) {
            std::lock_guard<std::mutex> lock(s_rid_cache_mu);
            s_rid_cache.erase(safe_string(_resc));
        }
        irods::catalog::resc_id_t rid = 0;
        if (_resc && g_catalog->resolve_resource_name(_resc, rid).ok()) {
            auto ret = g_catalog->delete_resource(rid);
            rodsLog(LOG_DEBUG, "L3_PLUGIN: EXITING db_del_resc_op SUCCESS");
            return ret;
        }
        rodsLog(LOG_DEBUG, "L3_PLUGIN: EXITING db_del_resc_op SUCCESS (no resc)");
        return SUCCESS();
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_del_resc_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

irods::error db_add_child_resc_op(irods::plugin_context& _ctx, std::map<std::string, std::string>* _resc_input) {
    try {
        rodsLog(LOG_DEBUG, "L3_PLUGIN: ENTERING db_add_child_resc_op");
        if (!_resc_input) return ERROR(SYS_INTERNAL_NULL_INPUT_ERR, "NULL resc_input");
        std::map<std::string, std::string>& resc_input = *_resc_input;

        std::string parent_name = resc_input[irods::RESOURCE_NAME];
        std::string new_child_string = resc_input[irods::RESOURCE_CHILDREN];

        irods::children_parser child_parser;
        child_parser.set_string(new_child_string);
        irods::children_parser::children_map_t c_map;
        child_parser.list(c_map);
        if (c_map.empty()) {
            return ERROR(SYS_INVALID_INPUT_PARAM, "child map is empty");
        }

        std::string child_name = c_map.begin()->first;
        std::string child_context = c_map.begin()->second;

        auto ret = g_catalog->add_child_resource(parent_name, child_name, child_context);
        rodsLog(LOG_DEBUG, "L3_PLUGIN: EXITING db_add_child_resc_op SUCCESS");
        return ret;
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_add_child_resc_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

irods::error db_del_child_resc_op(irods::plugin_context& _ctx, std::map<std::string, std::string>* _resc_input) {
    try {
        rodsLog(LOG_DEBUG, "L3_PLUGIN: ENTERING db_del_child_resc_op");
        if (!_resc_input) return ERROR(SYS_INTERNAL_NULL_INPUT_ERR, "NULL resc_input");
        std::map<std::string, std::string>& resc_input = *_resc_input;

        std::string parent_name = resc_input[irods::RESOURCE_NAME];
        std::string child_string = resc_input[irods::RESOURCE_CHILDREN];

        irods::children_parser parser;
        parser.set_string(child_string);
        std::string child_name;
        parser.first_child(child_name);

        auto ret = g_catalog->remove_child_resource(parent_name, child_name);
        rodsLog(LOG_DEBUG, "L3_PLUGIN: EXITING db_del_child_resc_op SUCCESS");
        return ret;
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_del_child_resc_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

irods::error db_gen_query_ticket_setup_op(
    irods::plugin_context& _ctx,
    const char*            _ticket,
    const char*            _client_addr)
{
    rodsLog(LOG_DEBUG, "L3_PLUGIN: ENTERING db_gen_query_ticket_setup_op ticket=%s, addr=%s",
            safe_string(_ticket).c_str(), safe_string(_client_addr).c_str());
    if (!_ticket || !_client_addr) {
        return ERROR(CAT_INVALID_ARGUMENT, "null parameter");
    }
    snprintf(g_session_ticket, sizeof(g_session_ticket), "%s", _ticket);
    snprintf(g_session_client_addr, sizeof(g_session_client_addr), "%s", _client_addr);
    if (_ctx.comm()) {
        snprintf(g_session_client_user, sizeof(g_session_client_user), "%s", _ctx.comm()->clientUser.userName);
        snprintf(g_session_client_zone, sizeof(g_session_client_zone), "%s", _ctx.comm()->clientUser.rodsZone);
    }
    return SUCCESS();
}

irods::error db_mod_ticket_op(
    irods::plugin_context& _ctx,
    const char*            _op_name,
    const char*            _ticket_string,
    const char*            _arg3,
    const char*            _arg4,
    const char*            _arg5,
    const KeyValPair*      _cond_input)
{
    try {
        using namespace irods::catalog;
        rodsLog(LOG_DEBUG, "L3_PLUGIN: ENTERING db_mod_ticket_op op=%s, ticket=%s", safe_string(_op_name).c_str(), safe_string(_ticket_string).c_str());
        if (!_op_name) return ERROR(SYS_INTERNAL_NULL_INPUT_ERR, "NULL op_name");

        if (strcmp(_op_name, "session") == 0) {
            const char* addr = (_arg3 && strlen(_arg3) > 0) ? _arg3 : (_ctx.comm() ? _ctx.comm()->clientAddr : "");
            return db_gen_query_ticket_setup_op(_ctx, _ticket_string ? _ticket_string : "", addr ? addr : "");
        }

        bool is_admin = false;
        if (_cond_input && getValByKey(_cond_input, ADMIN_KW)) {
            if (_ctx.comm() && !irods::is_privileged_client(*_ctx.comm())) {
                return ERROR(CAT_INSUFFICIENT_PRIVILEGE_LEVEL, "User is not a rodsadmin");
            }
            is_admin = true;
        }

        std::string client_user = (_ctx.comm() ? _ctx.comm()->clientUser.userName : "");
        if (client_user.empty()) client_user = g_zone_user;

        if (strcmp(_op_name, "create") == 0) {
            if (_ticket_string && isInteger(const_cast<char*>(_ticket_string))) {
                return ERROR(CAT_TICKET_INVALID, "ticket string cannot be a number");
            }
            std::string target_path = safe_string(_arg4);
            snowflake_id_t target_sid = 0;
            EntityType target_type = EntityType::DataObject;
            auto ret = g_catalog->resolve_path(target_path, target_sid, target_type);
            if (!ret.ok() || !target_sid) {
                if (!target_path.empty() && target_path.front() != '/') {
                    std::string full = "/" + std::string(_ctx.comm()->clientUser.rodsZone) + "/home/" +
                                       std::string(_ctx.comm()->clientUser.userName) + "/" + target_path;
                    if (g_catalog->resolve_path(full, target_sid, target_type).ok()) {
                        target_path = full;
                    }
                }
            }
            if (!target_sid) {
                target_sid = g_catalog->resolve_id_from_index(EntityType::DataObject, "n", target_path);
                if (target_sid) target_type = EntityType::DataObject;
            }
            if (!target_sid) {
                target_sid = g_catalog->resolve_id_from_index(EntityType::Collection, "n", target_path);
                if (target_sid) target_type = EntityType::Collection;
            }

            snowflake_id_t user_sid = g_catalog->resolve_id_from_index(EntityType::User, "n", client_user);
            if (!user_sid) user_sid = g_catalog->make_id(EntityType::User, 1);

            uint64_t ticket_id = 0;
            g_catalog->get_next_sequence_value("R_ObjectID", ticket_id);

            return g_catalog->create_ticket(ticket_id, safe_string(_ticket_string), safe_string(_arg3), target_sid, target_type, user_sid, client_user, target_path);
        }

        if (strcmp(_op_name, "delete") == 0) {
            return g_catalog->delete_ticket(safe_string(_ticket_string), client_user, is_admin);
        }

        if (strcmp(_op_name, "mod") == 0) {
            return g_catalog->modify_ticket(safe_string(_ticket_string), safe_string(_arg3), safe_string(_arg4), safe_string(_arg5), client_user, is_admin);
        }

        return SUCCESS();
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_mod_ticket_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

irods::error db_get_hierarchy_for_resc_op(irods::plugin_context& _ctx, const char* _resc_name, char** _hier) {
    try {
        rodsLog(LOG_DEBUG, "L3_PLUGIN: ENTERING db_get_hierarchy_for_resc_op");
        if (!_resc_name || !_hier) return ERROR(SYS_INVALID_INPUT_PARAM, "Null input");
        std::string hier;
        auto ret = g_catalog->get_hierarchy_for_resource(_resc_name, hier);
        if (ret.ok()) *_hier = strdup(hier.c_str());
        rodsLog(LOG_DEBUG, "L3_PLUGIN: EXITING db_get_hierarchy_for_resc_op SUCCESS");
        return ret;
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_get_hierarchy_for_resc_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

irods::error db_update_resc_obj_count(irods::plugin_context& _ctx, const std::string* _resc_name, int _delta) {
    try {
        rodsLog(LOG_DEBUG, "L3_PLUGIN: ENTERING db_update_resc_obj_count");
        if (!_resc_name) return ERROR(SYS_INVALID_INPUT_PARAM, "Null resource name");
        irods::catalog::snowflake_id_t rid = 0;
        if (!g_catalog->resolve_resource_name(*_resc_name, rid).ok()) return SUCCESS();
        auto ret = g_catalog->update_resource_object_count(rid, _delta);
        rodsLog(LOG_DEBUG, "L3_PLUGIN: EXITING db_update_resc_obj_count SUCCESS");
        return ret;
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_update_resc_obj_count EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

irods::error db_mod_resc_data_paths_op(irods::plugin_context& _ctx, const char* _resc, const char* _old_path, const char* _new_path, const char* _user) {
    rodsLog(LOG_DEBUG, "L3_PLUGIN: ENTERING db_mod_resc_data_paths_op resc [%s] old [%s] new [%s] user [%s]", safe_string(_resc).c_str(), safe_string(_old_path).c_str(), safe_string(_new_path).c_str(), safe_string(_user).c_str());
    return SUCCESS();
}

irods::error db_mod_resc_freespace_op(irods::plugin_context& _ctx, const char* _resc, const char* _freespace) {
    rodsLog(LOG_DEBUG, "L3_PLUGIN: ENTERING db_mod_resc_freespace_op resc [%s] freespace [%s]", safe_string(_resc).c_str(), safe_string(_freespace).c_str());
    return SUCCESS();
}

// Identity
irods::error db_reg_user_op(irods::plugin_context& _ctx, const irods::catalog::user& _user, irods::catalog::user_id_t* _out_id) {
    try {
        rodsLog(LOG_DEBUG, "L3_PLUGIN: ENTERING db_reg_user_op");
        if (!_out_id) return ERROR(SYS_INVALID_INPUT_PARAM, "null user_id_t pointer");
        auto ret = g_catalog->register_user(_user, *_out_id);
        rodsLog(LOG_DEBUG, "L3_PLUGIN: EXITING db_reg_user_op SUCCESS");
        return ret;
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_reg_user_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

irods::error db_reg_user_re_op(irods::plugin_context& _ctx, userInfo_t* _info) {
    try {
        rodsLog(LOG_DEBUG, "L3_PLUGIN: ENTERING db_reg_user_re_op");
        if (!_info) return ERROR(SYS_INVALID_INPUT_PARAM, "null userInfo_t");
        
        char userName2[NAME_LEN]{};
        char zoneName[NAME_LEN]{};
        int status = parseUserName(_info->userName, userName2, zoneName);
        if (status < 0) {
            return ERROR(status, "Invalid username format");
        }

        std::string zoneToUse;
        if (zoneName[0] != '\0') {
            zoneToUse = zoneName;
        } else if (_info->rodsZone[0] != '\0') {
            zoneToUse = _info->rodsZone;
        } else {
            try {
                zoneToUse = irods::server_properties::instance().map().get_json().at(KW_CFG_ZONE_NAME).get<std::string>();
            } catch (...) {
                zoneToUse = g_zone_name;
            }
        }

        irods::catalog::user user;
        user.name = userName2;
        user.type = safe_string(_info->userType);
        user.zone = zoneToUse;

        if (_info->sysUid > 0) {
            user.id = (uint64_t)_info->sysUid;
        } else {
            g_catalog->get_next_sequence_value("R_USER_MAIN", user.id);
        }

        rodsLog(LOG_DEBUG, "L3_PLUGIN: db_reg_user_re_op: registering user [%s] type [%s] zone [%s] id [%lu]", user.name.c_str(), user.type.c_str(), user.zone.c_str(), user.id);
        
        irods::catalog::user_id_t out_id;
        auto ret = g_catalog->register_user(user, out_id);
        if (ret.ok()) {
            _info->sysUid = (int)out_id;
            // NOTE: Do not overwrite _info->userName with userName2.
            // When user is remote (e.g. rods#remoteZone), iRODS rules expect _info->userName to retain
            // the full name ("rods#remoteZone") so that $otherUserName in core.re expands to "rods#remoteZone"
            // and creates /tempZone/home/rods#remoteZone instead of colliding with /tempZone/home/rods!
            // Postgres plugin does not overwrite _user_info->userName.
            if (_info->rodsZone[0] == '\0') {
                rstrcpy(_info->rodsZone, zoneToUse.c_str(), NAME_LEN);
            }
            rodsLog(LOG_DEBUG, "L3_PLUGIN: EXITING db_reg_user_re_op SUCCESS");
        } else {
            rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_reg_user_re_op ERROR: %ld - %s", ret.code(), ret.result().c_str());
        }
        return ret;
    } catch (const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_reg_user_re_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

static char prevChalSig[200] = {0};

static int decodePw(rsComm_t* rsComm, const char* in, char* out) {
    if (!in || !out) return -1;
    char password[MAX_PASSWORD_LEN]{};
    char upassword[MAX_PASSWORD_LEN + 10]{};
    char rand_pad[] = "1gCBizHWbwIYyWLo";  /* must match clients */

    std::string client_user = (rsComm && rsComm->clientUser.userName[0] != '\0') ? safe_string(rsComm->clientUser.userName) : g_zone_user;
    std::string client_zone = (rsComm && rsComm->clientUser.rodsZone[0] != '\0') ? safe_string(rsComm->clientUser.rodsZone) : "";
    if (client_zone.empty()) {
        try {
            client_zone = irods::server_properties::instance().map().get_json().at(KW_CFG_ZONE_NAME).get<std::string>();
        } catch (...) {
            client_zone = g_zone_name;
        }
    }
    std::string stored_caller_pw;
    int priv = 0;
    auto ret = g_catalog->get_user_password_and_priv(client_user, client_zone, stored_caller_pw, priv);
    if (!ret.ok()) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: decodePw: failed to get password for user [%s#%s]", client_user.c_str(), client_zone.c_str());
        return CAT_INVALID_AUTHENTICATION;
    }
    rstrcpy(password, stored_caller_pw.c_str(), sizeof(password));

    obfDecodeByKeyV2(in, password, prevChalSig, upassword);

    size_t pwLen1 = strlen(upassword);
    memset(password, 0, sizeof(password));

    char* cp = strstr(upassword, rand_pad);
    if (cp != NULL) {
        *cp = '\0';
    }

    size_t pwLen2 = strlen(upassword);

    if (pwLen2 > MAX_PASSWORD_LEN - 5 && pwLen2 == pwLen1) {
        /* probable failure */
        rodsLog(LOG_ERROR, "L3_PLUGIN: decodePw: password encoding error for user [%s]", client_user.c_str());
        if (rsComm) {
            addRErrorMsg(
                &rsComm->rError,
                0,
                "Error with password encoding.  This can be caused by not connecting directly to the ICAT host, not using password authentication (using GSI or Kerberos instead), or entering your password incorrectly (if prompted)." );
        }
        return CAT_PASSWORD_ENCODING_ERROR;
    }
    strcpy(out, upassword);
    memset(upassword, 0, sizeof(upassword));

    return 0;
}

irods::error db_mod_user_op(irods::plugin_context& _ctx, const char* _user, const char* _option, const char* _value) {
    try {
        rodsLog(LOG_DEBUG, "L3_PLUGIN: ENTERING db_mod_user_op user [%s] opt [%s]", safe_string(_user).c_str(), safe_string(_option).c_str());
        if (!_user || !_option || !_value) {
            return ERROR(SYS_INVALID_INPUT_PARAM, "null parameter in db_mod_user_op");
        }
        std::string opt = safe_string(_option);
        std::string val = safe_string(_value);

        if (opt == "password") {
            char decoded_password[MAX_PASSWORD_LEN + 10]{};
            int ec = decodePw(_ctx.comm(), _value, decoded_password);
            if (ec < 0) {
                rodsLog(LOG_ERROR, "L3_PLUGIN: db_mod_user_op: decodePw failed with error %d", ec);
                return ERROR(ec, "decodePw failed");
            }
            val = decoded_password;
        } else if (opt == "password-unobfuscated") {
            opt = "password";
        }

        auto ret = g_catalog->modify_user(safe_string(_user), opt, val);
        rodsLog(LOG_DEBUG, "L3_PLUGIN: EXITING db_mod_user_op SUCCESS");
        return ret;
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_mod_user_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

irods::error db_del_user_op(irods::plugin_context& _ctx, const char* _username) {
    try {
        rodsLog(LOG_DEBUG, "L3_PLUGIN: ENTERING db_del_user_op");
        if (!_username) return ERROR(SYS_INVALID_INPUT_PARAM, "null username");
        char userName2[NAME_LEN]{};
        char zoneName[NAME_LEN]{};
        parseUserName(_username, userName2, zoneName);
        auto ret = g_catalog->delete_user(userName2, zoneName);
        rodsLog(LOG_DEBUG, "L3_PLUGIN: EXITING db_del_user_op SUCCESS");
        return ret;
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_del_user_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

irods::error db_del_user_re_op(irods::plugin_context& _ctx, userInfo_t* _info) {
    try {
        rodsLog(LOG_DEBUG, "L3_PLUGIN: ENTERING db_del_user_re_op");
        if (!_info) return ERROR(SYS_INVALID_INPUT_PARAM, "null userInfo_t");

        if (_ctx.comm()->clientUser.authInfo.authFlag < LOCAL_PRIV_USER_AUTH) {
            return ERROR(CAT_INSUFFICIENT_PRIVILEGE_LEVEL, "insufficient privilege");
        }
        if (_ctx.comm()->proxyUser.authInfo.authFlag < LOCAL_PRIV_USER_AUTH) {
            return ERROR(CAT_INSUFFICIENT_PRIVILEGE_LEVEL, "insufficient privilege level");
        }

        std::string zoneToUse;
        try {
            zoneToUse = irods::server_properties::instance().map().get_json().at(KW_CFG_ZONE_NAME).get<std::string>();
        } catch (...) {
            zoneToUse = g_zone_name;
        }
        if (_info->rodsZone[0] != '\0') {
            zoneToUse = _info->rodsZone;
        }

        char userName2[NAME_LEN]{};
        char zoneName[NAME_LEN]{};
        int status = parseUserName(_info->userName, userName2, zoneName);
        if (status < 0) {
            return ERROR(status, "Invalid username format");
        }
        if (zoneName[0] != '\0') {
            zoneToUse = zoneName;
        }

        if (strncmp(_ctx.comm()->clientUser.userName, userName2, sizeof(userName2)) == 0 &&
            strncmp(_ctx.comm()->clientUser.rodsZone, zoneToUse.c_str(), zoneToUse.size()) == 0) {
            addRErrorMsg(&_ctx.comm()->rError, 0, "Cannot remove your own admin account, probably unintended");
            return ERROR(CAT_INVALID_USER, "invalid user");
        }

        rodsLog(LOG_DEBUG, "L3_PLUGIN: db_del_user_re_op: deleting user [%s#%s]", userName2, zoneToUse.c_str());
        
        auto ret = g_catalog->delete_user(userName2, zoneToUse);
        
        rodsLog(LOG_DEBUG, "L3_PLUGIN: EXITING db_del_user_re_op SUCCESS");
        return ret;
    } catch (const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_del_user_re_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

irods::error db_check_auth_op(irods::plugin_context& _ctx, const char* _scheme, const char* _challenge, const char* _response, const char* _user_name, int* _user_priv_level, int* _client_priv_level) {
    try {
        rodsLog(LOG_DEBUG, "L3_PLUGIN: ENTERING db_check_auth_op user [%s]", safe_string(_user_name).c_str());
        if (!_challenge || !_response || !_user_name || !_user_priv_level || !_client_priv_level) {
            return ERROR(CAT_INVALID_ARGUMENT, "null parameter in db_check_auth_op");
        }

        *_user_priv_level = NO_USER_AUTH;
        *_client_priv_level = NO_USER_AUTH;

        int hashType = HASH_TYPE_MD5;
        std::string user_str(_user_name);
        auto pos_sha = user_str.find(SHA1_FLAG_STRING);
        if (std::string::npos != pos_sha) {
            user_str = user_str.substr(0, pos_sha);
            hashType = HASH_TYPE_SHA1;
        }

        char md5Buf[CHALLENGE_LEN + MAX_PASSWORD_LEN + 2]{};
        strncpy(md5Buf, _challenge, CHALLENGE_LEN);
        snprintf(prevChalSig, sizeof(prevChalSig),
                  "%2.2x%2.2x%2.2x%2.2x%2.2x%2.2x%2.2x%2.2x%2.2x%2.2x%2.2x%2.2x%2.2x%2.2x%2.2x%2.2x",
                  (unsigned char)md5Buf[0], (unsigned char)md5Buf[1],
                  (unsigned char)md5Buf[2], (unsigned char)md5Buf[3],
                  (unsigned char)md5Buf[4], (unsigned char)md5Buf[5],
                  (unsigned char)md5Buf[6], (unsigned char)md5Buf[7],
                  (unsigned char)md5Buf[8], (unsigned char)md5Buf[9],
                  (unsigned char)md5Buf[10], (unsigned char)md5Buf[11],
                  (unsigned char)md5Buf[12], (unsigned char)md5Buf[13],
                  (unsigned char)md5Buf[14], (unsigned char)md5Buf[15]);

        std::string user_name = user_str, zone_name = "";
        auto pos = user_str.find('#');
        if (pos != std::string::npos) {
            user_name = user_str.substr(0, pos);
            zone_name = user_str.substr(pos + 1);
        } else {
            zone_name = irods::server_properties::instance().map().get_json().at(KW_CFG_ZONE_NAME).get<std::string>();
        }

        bool isAnonymous = (strncmp(ANONYMOUS_USER, user_name.c_str(), NAME_LEN) == 0);

        std::string stored_pw;
        int priv = 0;
        auto get_pw_res = g_catalog->get_user_password_and_priv(user_name, zone_name, stored_pw, priv);
        if (!get_pw_res.ok()) {
            rodsLog(LOG_ERROR, "L3_PLUGIN: db_check_auth_op: get_user_password_and_priv failed for [%s#%s]: %s", user_name.c_str(), zone_name.c_str(), get_pw_res.result().c_str());
            return ERROR(CAT_INVALID_AUTHENTICATION, "User not found or password lookup failed");
        }

        if (!isAnonymous) {
            memset(md5Buf, 0, sizeof(md5Buf));
            strncpy(md5Buf, _challenge, CHALLENGE_LEN);
            strncpy(md5Buf + CHALLENGE_LEN, stored_pw.c_str(), MAX_PASSWORD_LEN);

            char digest[RESPONSE_LEN + 2]{};
            obfMakeOneWayHash(hashType,
                              reinterpret_cast<unsigned char*>(md5Buf),
                              CHALLENGE_LEN + MAX_PASSWORD_LEN,
                              reinterpret_cast<unsigned char*>(digest));

            for (int i = 0; i < RESPONSE_LEN; i++) {
                if (digest[i] == '\0') {
                    digest[i]++;
                }
            }

            const char* cp = _response;
            int OK = 1;
            for (int i = 0; i < RESPONSE_LEN; i++) {
                if (*cp++ != digest[i]) {
                    OK = 0;
                    break;
                }
            }

            if (OK == 0) {
                rodsLog(LOG_DEBUG, "L3_PLUGIN: db_check_auth_op: Authentication failed for user [%s]", user_name.c_str());
                return ERROR(CAT_INVALID_AUTHENTICATION, "Authentication failed");
            }
        }

        if (priv == 1) {
            *_user_priv_level = LOCAL_USER_AUTH;
        } else if (priv == 5) {
            *_user_priv_level = LOCAL_PRIV_USER_AUTH;
        } else {
            *_user_priv_level = priv;
        }
        *_client_priv_level = *_user_priv_level;

        rodsLog(LOG_DEBUG, "L3_PLUGIN: EXITING db_check_auth_op SUCCESS (priv=%d)", *_user_priv_level);
        return SUCCESS();
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_check_auth_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

irods::error db_check_auth_credentials_op(irods::plugin_context& _ctx, const char* _username, const char* _zone, const char* _password, int* _correct) {
    try {
        rodsLog(LOG_DEBUG, "L3_PLUGIN: ENTERING db_check_auth_credentials_op user [%s]", safe_string(_username).c_str());
        if (!_username || !_zone || !_password || !_correct) {
            return ERROR(SYS_INVALID_INPUT_PARAM, "null parameter in db_check_auth_credentials_op");
        }
        *_correct = -1;
        char decoded_password[MAX_PASSWORD_LEN + 20]{};
        if (const auto ec = decodePw(_ctx.comm(), _password, decoded_password); ec < 0) {
            rodsLog(LOG_ERROR, "L3_PLUGIN: db_check_auth_credentials_op: decodePw failed with error %d", ec);
            return ERROR(ec, "Password decode error");
        }
        bool correct = false;
        auto ret = g_catalog->check_auth_credentials(safe_string(_username), safe_string(_zone), decoded_password, correct);
        *_correct = correct ? 1 : 0;
        rodsLog(LOG_DEBUG, "L3_PLUGIN: EXITING db_check_auth_credentials_op SUCCESS (correct=%d)", *_correct);
        return ret;
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_check_auth_credentials_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

irods::error db_update_pam_password_op(irods::plugin_context& _ctx,
                                       const char* _user_name,
                                       int _ttl,
                                       const char* _test_time,
                                       char** _password_buffer,
                                       std::size_t _password_buffer_size) {
    try {
        rodsLog(LOG_DEBUG, "L3_PLUGIN: ENTERING db_update_pam_password_op user [%s]", safe_string(_user_name).c_str());
        if (!_user_name || !_password_buffer || !*_password_buffer) {
            return ERROR(CAT_INVALID_ARGUMENT, "null parameter in db_update_pam_password_op");
        }

        constexpr std::size_t random_password_len = MAX_PASSWORD_LEN - 8;
        if (random_password_len + 1 > _password_buffer_size) {
            return ERROR(SYS_INVALID_INPUT_PARAM, "Buffer not large enough");
        }

        const auto random_password = irods::generate_random_alphanumeric_string(random_password_len);
        auto ret = g_catalog->modify_user(safe_string(_user_name), "password", random_password);
        if (!ret.ok()) {
            rodsLog(LOG_ERROR, "L3_PLUGIN: db_update_pam_password_op modify_user failed: %s", ret.result().c_str());
            return ret;
        }

        std::strncpy(*_password_buffer, random_password.c_str(), _password_buffer_size);
        rodsLog(LOG_DEBUG, "L3_PLUGIN: EXITING db_update_pam_password_op SUCCESS");
        return SUCCESS();
    } catch (const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_update_pam_password_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

irods::error db_make_temp_pw_op(irods::plugin_context& _ctx,
                                char* _pw_value_to_hash,
                                const char* _other_user) {
    try {
        rodsLog(LOG_DEBUG, "L3_PLUGIN: ENTERING db_make_temp_pw_op other_user [%s]", safe_string(_other_user).c_str());
        if (!_pw_value_to_hash || !_other_user) {
            return ERROR(CAT_INVALID_ARGUMENT, "null parameter in db_make_temp_pw_op");
        }
        std::string target_user = safe_string(_other_user);
        if (target_user.empty()) {
            target_user = _ctx.comm() ? safe_string(_ctx.comm()->clientUser.userName) : g_zone_user;
        }
        std::string zone = _ctx.comm() ? safe_string(_ctx.comm()->clientUser.rodsZone) : "";
        if (zone.empty()) {
            zone = irods::server_properties::instance().map().get_json().at(KW_CFG_ZONE_NAME).get<std::string>();
        }

        std::string stored_caller_pw;
        int priv = 0;
        auto ret = g_catalog->get_user_password_and_priv(target_user, zone, stored_caller_pw, priv);
        if (!ret.ok()) {
            return ERROR(CAT_INVALID_USER, "user not found");
        }

        const auto random_alphanumeric = irods::generate_random_alphanumeric_string(MAX_PASSWORD_LEN - 8);
        snprintf(_pw_value_to_hash, MAX_PASSWORD_LEN, "%s", random_alphanumeric.c_str());

        char md5Buf[100]{};
        snprintf(md5Buf, sizeof(md5Buf), "%s%s", random_alphanumeric.c_str(), stored_caller_pw.c_str());
        unsigned char digest[RESPONSE_LEN + 2]{};
        obfMakeOneWayHash(HASH_TYPE_DEFAULT, reinterpret_cast<unsigned char*>(md5Buf), 100, digest);

        char newPw[MAX_PASSWORD_LEN + 10]{};
        hashToStr(digest, newPw);

        g_catalog->modify_user(target_user, "password", newPw);

        rodsLog(LOG_DEBUG, "L3_PLUGIN: EXITING db_make_temp_pw_op SUCCESS");
        return SUCCESS();
    } catch (const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_make_temp_pw_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

irods::error db_make_limited_pw_op(irods::plugin_context& _ctx,
                                   int _ttl,
                                   char* _pw_value_to_hash) {
    try {
        rodsLog(LOG_DEBUG, "L3_PLUGIN: ENTERING db_make_limited_pw_op");
        if (!_pw_value_to_hash) {
            return ERROR(CAT_INVALID_ARGUMENT, "null parameter in db_make_limited_pw_op");
        }
        std::string target_user = _ctx.comm() ? safe_string(_ctx.comm()->clientUser.userName) : g_zone_user;
        std::string zone = _ctx.comm() ? safe_string(_ctx.comm()->clientUser.rodsZone) : "";
        if (zone.empty()) {
            zone = irods::server_properties::instance().map().get_json().at(KW_CFG_ZONE_NAME).get<std::string>();
        }

        std::string stored_caller_pw;
        int priv = 0;
        auto ret = g_catalog->get_user_password_and_priv(target_user, zone, stored_caller_pw, priv);
        if (!ret.ok()) {
            return ERROR(CAT_INVALID_USER, "user not found");
        }

        const auto random_alphanumeric = irods::generate_random_alphanumeric_string(MAX_PASSWORD_LEN - 8);
        snprintf(_pw_value_to_hash, MAX_PASSWORD_LEN, "%s", random_alphanumeric.c_str());

        char md5Buf[100]{};
        snprintf(md5Buf, sizeof(md5Buf), "%s%s", random_alphanumeric.c_str(), stored_caller_pw.c_str());
        unsigned char digest[RESPONSE_LEN + 2]{};
        obfMakeOneWayHash(HASH_TYPE_DEFAULT, reinterpret_cast<unsigned char*>(md5Buf), 100, digest);

        char newPw[MAX_PASSWORD_LEN + 10]{};
        hashToStr(digest, newPw);

        g_catalog->modify_user(target_user, "password", newPw);

        rodsLog(LOG_DEBUG, "L3_PLUGIN: EXITING db_make_limited_pw_op SUCCESS");
        return SUCCESS();
    } catch (const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_make_limited_pw_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

irods::error db_mod_group_op(irods::plugin_context& _ctx, const char* _group, const char* _option, const char* _user, const char* _zone) {
    std::string zone_str = safe_string(_zone);
    if (zone_str.empty()) {
        const auto& config = irods::server_properties::instance().map().get_json();
        zone_str = config.at(KW_CFG_ZONE_NAME).get<std::string>();
    }
    rodsLog(LOG_DEBUG, "L3_PLUGIN: ENTERING db_mod_group_op group [%s] opt [%s] user [%s] zone [%s]", safe_string(_group).c_str(), safe_string(_option).c_str(), safe_string(_user).c_str(), zone_str.c_str());
    try {
        if (_option && std::string(_option) == "add") {
            auto ret = g_catalog->add_user_to_group(safe_string(_user), zone_str, safe_string(_group));
            rodsLog(LOG_DEBUG, "L3_PLUGIN: EXITING db_mod_group_op SUCCESS");
            return ret;
        } else if (_option && std::string(_option) == "remove") {
            auto ret = g_catalog->remove_user_from_group(safe_string(_user), zone_str, safe_string(_group));
            rodsLog(LOG_DEBUG, "L3_PLUGIN: EXITING db_mod_group_op SUCCESS");
            return ret;
        }
        rodsLog(LOG_DEBUG, "L3_PLUGIN: EXITING db_mod_group_op SUCCESS (no opt)");
        return SUCCESS();
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_mod_group_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

// Metadata
static irods::error check_avu_access(
    irods::plugin_context& _ctx,
    const char* _type,
    const char* _name,
    const char* _req_level,
    const KeyValPair* _cond_input,
    irods::catalog::snowflake_id_t& out_target_sid)
{
    out_target_sid = 0;
    if (!_type || *_type == '\0') {
        return ERROR(CAT_INVALID_ARGUMENT, "type null or empty");
    }
    if (!_name || *_name == '\0') {
        return ERROR(CAT_INVALID_ARGUMENT, "name null or empty");
    }

    if (!_ctx.comm()) {
        return SUCCESS();
    }

    const bool admin_mode = _cond_input && getValByKey(_cond_input, ADMIN_KW);
    const bool is_privileged = irods::is_privileged_client(*_ctx.comm());

    if (admin_mode && !is_privileged) {
        return ERROR(CAT_INSUFFICIENT_PRIVILEGE_LEVEL, "Insufficient privileges");
    }

    std::string type_str = safe_string(_type);

    // Resources
    if (type_str == "-r" || type_str == "-R" || type_str == "Resource" || type_str == "resc") {
        if (!is_privileged) {
            return ERROR(CAT_INSUFFICIENT_PRIVILEGE_LEVEL, "Insufficient privileges");
        }
        out_target_sid = g_catalog->resolve_target_entity_sid(type_str, _name);
        if (!out_target_sid) {
            return ERROR(CAT_INVALID_RESOURCE, "invalid resource");
        }
        return SUCCESS();
    }

    // Users
    if (type_str == "-u" || type_str == "-U" || type_str == "User" || type_str == "user") {
        if (!is_privileged) {
            return ERROR(CAT_INSUFFICIENT_PRIVILEGE_LEVEL, "Insufficient privileges");
        }
        out_target_sid = g_catalog->resolve_target_entity_sid(type_str, _name);
        if (!out_target_sid) {
            return ERROR(CAT_INVALID_USER, "invalid user");
        }
        return SUCCESS();
    }

    // Data Objects & Collections
    bool is_data_obj = (type_str == "-d" || type_str == "-D" || type_str == "DataObject" || type_str == "data");
    bool is_coll = (type_str == "-c" || type_str == "-C" || type_str == "Collection" || type_str == "coll");
    if (!is_data_obj && !is_coll) {
        return ERROR(CAT_INVALID_ARGUMENT, "invalid type argument");
    }

    out_target_sid = g_catalog->resolve_target_entity_sid(type_str, _name);
    if (!out_target_sid) {
        if (is_data_obj) {
            return ERROR(CAT_UNKNOWN_FILE, "data object not found");
        } else {
            return ERROR(CAT_UNKNOWN_COLLECTION, "collection not found");
        }
    }

    if (admin_mode && is_privileged) {
        return SUCCESS();
    }

    std::string user_name = safe_string(_ctx.comm()->clientUser.userName);
    if (user_name.empty()) {
        return ERROR(CAT_NO_ACCESS_PERMISSION, "User does not have permission");
    }

    irods::catalog::snowflake_id_t usid = 0;
    if (!g_catalog->resolve_user_name(user_name, usid).ok() || !usid) {
        return ERROR(CAT_INVALID_USER, "invalid user");
    }

    bool allowed = false;
    g_catalog->check_permission(usid, out_target_sid, safe_string(_req_level), allowed);
    if (!allowed) {
        rodsLog(LOG_DEBUG, "L3_PLUGIN: check_avu_access: Access Denied for user [%s] on object [%s] with level [%s]",
                user_name.c_str(), safe_string(_name).c_str(), safe_string(_req_level).c_str());
        return ERROR(CAT_NO_ACCESS_PERMISSION, "User does not have permission");
    }

    return SUCCESS();
}

irods::error db_add_avu_metadata_op(irods::plugin_context& _ctx, const char* _type, const char* _target_id, const char* _attr, const char* _val, const char* _units, const KeyValPair* _cond_input) {
    try {
        rodsLog(LOG_DEBUG, "L3_PLUGIN: ENTERING db_add_avu_metadata_op");
        irods::catalog::snowflake_id_t target_sid = 0;
        auto access_ret = check_avu_access(_ctx, _type, _target_id, "write", _cond_input, target_sid);
        if (!access_ret.ok()) {
            return access_ret;
        }
        auto ret = g_catalog->add_avu_metadata(safe_string(_type), safe_string(_target_id), {safe_string(_attr), safe_string(_val), safe_string(_units)});
        rodsLog(LOG_DEBUG, "L3_PLUGIN: EXITING db_add_avu_metadata_op SUCCESS");
        return ret;
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_add_avu_metadata_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

static int check_mod_arg_type(const char* arg) {
    if (arg == nullptr || *arg == '\0') {
        return CAT_INVALID_ARGUMENT;
    }
    if (arg[1] != ':') {
        return 0;
    }
    switch (arg[0]) {
        case 'n': return 1;
        case 'v': return 2;
        case 'u': return 3;
        default: return 0;
    }
}

irods::error db_del_avu_metadata_op(irods::plugin_context& _ctx, int _option, const char* _type, const char* _target_id, const char* _attr, const char* _val, const char* _units, int _nocommit, const KeyValPair* _cond_input) {
    try {
        rodsLog(LOG_DEBUG, "L3_PLUGIN: ENTERING db_del_avu_metadata_op");
        irods::catalog::snowflake_id_t target_sid = 0;
        auto access_ret = check_avu_access(_ctx, _type, _target_id, "write", _cond_input, target_sid);
        if (!access_ret.ok()) {
            return access_ret;
        }
        auto ret = g_catalog->delete_avu_metadata(safe_string(_type), safe_string(_target_id), {safe_string(_attr), safe_string(_val), safe_string(_units)}, _option);
        rodsLog(LOG_DEBUG, "L3_PLUGIN: EXITING db_del_avu_metadata_op SUCCESS");
        return ret;
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_del_avu_metadata_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

irods::error db_mod_avu_metadata_op(
    irods::plugin_context& _ctx,
    const char* _type,
    const char* _name,
    const char* _attribute,
    const char* _value,
    const char* _unitsOrArg0,
    const char* _arg1,
    const char* _arg2,
    const char* _arg3,
    const KeyValPair* _cond_input)
{
    try {
        rodsLog(LOG_DEBUG, "L3_PLUGIN: ENTERING db_mod_avu_metadata_op");

        if (!_type || !_name || !_attribute || !_value) {
            return ERROR(CAT_INVALID_ARGUMENT, "Null parameter in db_mod_avu_metadata_op");
        }

        irods::catalog::snowflake_id_t target_sid = 0;
        auto access_ret = check_avu_access(_ctx, _type, _name, "write", _cond_input, target_sid);
        if (!access_ret.ok()) {
            return access_ret;
        }

        if (_unitsOrArg0 == nullptr) {
            return ERROR(CAT_INVALID_ARGUMENT, "unitsOrArg0 empty or null");
        }

        const char* dummy = nullptr;
        const char* myUnits = "";
        const char* addAttr = "";
        const char* addValue = "";
        const char* addUnits = nullptr;

        int atype = check_mod_arg_type(_unitsOrArg0);
        if (atype == 0) {
            myUnits = _unitsOrArg0;
        } else {
            dummy = _unitsOrArg0;
        }

        bool new_attr_set = false;
        bool new_val_set = false;
        bool new_unit_set = false;

        for (auto arg : { dummy, _arg1, _arg2, _arg3 }) {
            if (arg == nullptr || *arg == '\0') {
                continue;
            }
            atype = check_mod_arg_type(arg);
            if (atype == 1) { // n:
                if (new_attr_set) {
                    return ERROR(CAT_INVALID_ARGUMENT, "new attribute specified more than once");
                }
                new_attr_set = true;
                addAttr = arg + 2;
            } else if (atype == 2) { // v:
                if (new_val_set) {
                    return ERROR(CAT_INVALID_ARGUMENT, "new value specified more than once");
                }
                new_val_set = true;
                addValue = arg + 2;
            } else if (atype == 3) { // u:
                if (new_unit_set) {
                    return ERROR(CAT_INVALID_ARGUMENT, "new unit specified more than once");
                }
                new_unit_set = true;
                addUnits = arg + 2;
            }
        }

        if (*addAttr == '\0' && *addValue == '\0' && addUnits == nullptr) {
            return ERROR(CAT_INVALID_ARGUMENT, "arg check failed");
        }

        if (*addAttr == '\0') {
            addAttr = _attribute;
        }
        if (*addValue == '\0') {
            addValue = _value;
        }
        if (addUnits == nullptr) {
            addUnits = myUnits;
        }

        auto ret = g_catalog->modify_avu_metadata(
            safe_string(_type),
            safe_string(_name),
            {safe_string(_attribute), safe_string(_value), safe_string(myUnits)},
            {safe_string(addAttr), safe_string(addValue), safe_string(addUnits)});

        rodsLog(LOG_DEBUG, "L3_PLUGIN: EXITING db_mod_avu_metadata_op SUCCESS");
        return ret;
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_mod_avu_metadata_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

irods::error db_copy_avu_metadata_op(
    irods::plugin_context& _ctx,
    const char* _type1,
    const char* _type2,
    const char* _name1,
    const char* _name2,
    const KeyValPair* _cond_input)
{
    try {
        rodsLog(LOG_DEBUG, "L3_PLUGIN: ENTERING db_copy_avu_metadata_op");
        if (!_type1 || !_type2 || !_name1 || !_name2) {
            return ERROR(CAT_INVALID_ARGUMENT, "null parameter in db_copy_avu_metadata_op");
        }
        irods::catalog::snowflake_id_t src_sid = 0;
        auto ret1 = check_avu_access(_ctx, _type1, _name1, "read", _cond_input, src_sid);
        if (!ret1.ok()) {
            return ret1;
        }
        irods::catalog::snowflake_id_t dst_sid = 0;
        auto ret2 = check_avu_access(_ctx, _type2, _name2, "write", _cond_input, dst_sid);
        if (!ret2.ok()) {
            return ret2;
        }
        auto ret = g_catalog->copy_avu_metadata(safe_string(_type1), safe_string(_name1), safe_string(_type2), safe_string(_name2));
        rodsLog(LOG_DEBUG, "L3_PLUGIN: EXITING db_copy_avu_metadata_op SUCCESS");
        return ret;
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_copy_avu_metadata_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

irods::error db_set_avu_metadata_op(irods::plugin_context& _ctx, const char* _type, const char* _target_id, const char* _attr, const char* _val, const char* _units, const KeyValPair* _cond_input) {
    try {
        rodsLog(LOG_DEBUG, "L3_PLUGIN: ENTERING db_set_avu_metadata_op");
        irods::catalog::snowflake_id_t target_sid = 0;
        auto access_ret = check_avu_access(_ctx, _type, _target_id, "write", _cond_input, target_sid);
        if (!access_ret.ok()) {
            return access_ret;
        }
        auto ret = g_catalog->set_avu_metadata(safe_string(_type), safe_string(_target_id), {safe_string(_attr), safe_string(_val), safe_string(_units)});
        rodsLog(LOG_DEBUG, "L3_PLUGIN: EXITING db_set_avu_metadata_op SUCCESS");
        return ret;
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_set_avu_metadata_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

irods::error db_mod_access_control_op(irods::plugin_context& _ctx, int _recursive, const char* _access_level, const char* _user, const char* _zone, const char* _path) {
    try {
        rodsLog(LOG_DEBUG, "L3_PLUGIN: ENTERING db_mod_access_control_op");
        auto ret = g_catalog->set_access(safe_string(_user), safe_string(_zone), safe_string(_path), safe_string(_access_level), _recursive != 0);
        rodsLog(LOG_DEBUG, "L3_PLUGIN: EXITING db_mod_access_control_op SUCCESS");
        return ret;
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_mod_access_control_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

irods::error db_check_permission_to_modify_data_object_op(irods::plugin_context& _ctx, rodsLong_t _data_id) {
    try {
        rodsLog(LOG_DEBUG, "L3_PLUGIN: ENTERING db_check_permission_to_modify_data_object_op id [%ld]", _data_id);
        if (_ctx.comm() && irods::is_privileged_client(*_ctx.comm())) {
            return SUCCESS();
        }
        bool allowed = false;
        
        std::string user_name;
        if (_ctx.comm()) {
            user_name = _ctx.comm()->clientUser.userName;
        } else {
            const auto& config = irods::server_properties::instance().map().get_json();
            if (config.contains(KW_CFG_ZONE_USER)) {
                user_name = config.at(KW_CFG_ZONE_USER).get<std::string>();
            }
        }
        irods::catalog::snowflake_id_t usid = 0;
        irods::catalog::snowflake_id_t dsid = g_catalog->make_id(irods::catalog::EntityType::DataObject, (uint64_t)_data_id);
        if (g_session_ticket[0] != '\0') {
            std::string cuser = user_name.empty() ? g_session_client_user : user_name;
            auto terr = g_catalog->check_ticket_access(g_session_ticket, dsid, "write", cuser, g_session_client_addr);
            if (!terr.ok()) {
                return terr;
            }
            allowed = true;
        } else {
            if (g_catalog->resolve_user_name(user_name, usid).ok()) {
                g_catalog->check_permission_to_modify_data_object(usid, dsid, allowed);
            }
        }

        rodsLog(LOG_DEBUG, "L3_PLUGIN: EXITING db_check_permission_to_modify_data_object_op id [%ld] allowed [%d]", _data_id, allowed);
        if (allowed) return SUCCESS();
        rodsLog(LOG_ERROR, "L3_PLUGIN: Access Denied for user [%s] on object [%ld]", user_name.c_str(), _data_id);
        return ERROR(CAT_NO_ACCESS_PERMISSION, "User does not have permission to modify data object");
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_check_permission_to_modify_data_object_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

irods::error db_update_ticket_write_byte_count_op(irods::plugin_context& _ctx, rodsLong_t _data_id, rodsLong_t _bytes) {
    rodsLog(LOG_DEBUG, "L3_PLUGIN: ENTERING db_update_ticket_write_byte_count_op id [%ld] bytes [%ld]", _data_id, _bytes);
    if (g_session_ticket[0] != '\0') {
        irods::catalog::snowflake_id_t dsid = g_catalog->make_id(irods::catalog::EntityType::DataObject, (uint64_t)_data_id);
        g_catalog->update_ticket_write_bytes(g_session_ticket, dsid, _bytes);
    }
    rodsLog(LOG_DEBUG, "L3_PLUGIN: EXITING db_update_ticket_write_byte_count_op SUCCESS");
    return SUCCESS();
}

irods::error db_data_object_finalize_op(irods::plugin_context& _ctx, const char* _path) {
    try {
        std::string json_str = safe_string(_path);
        rodsLog(LOG_DEBUG, "L3_PLUGIN: ENTERING db_data_object_finalize_op [%s]", json_str.c_str());
        
        if (json_str.empty()) return SUCCESS();

        auto j = lite3cpp::lite3_json::from_json_string(json_str);
        if (j.size() >= sizeof(lite3cpp::PackedNodeLayout) && j.get_type(0, "replicas") == lite3cpp::Type::Array) {
            size_t repls_ofs = j.get_arr(0, "replicas");
            lite3cpp::NodeView rnv(reinterpret_cast<const lite3cpp::PackedNodeLayout*>(j.data() + repls_ofs));
            for (uint32_t ri = 0; ri < rnv.size(); ++ri) {
                if (j.arr_get_type(repls_ofs, ri) != lite3cpp::Type::Object) continue;
                size_t r_ofs = j.arr_get_obj(repls_ofs, ri);
                if (j.get_type(r_ofs, "after") == lite3cpp::Type::Object) {
                    size_t after_ofs = j.get_obj(r_ofs, "after");
                    
                    auto get_after_u64 = [&](std::string_view key) -> uint64_t {
                        auto t = j.get_type(after_ofs, key);
                        if (t == lite3cpp::Type::Int64) return static_cast<uint64_t>(j.get_i64(after_ofs, key));
                        if (t == lite3cpp::Type::String) {
                            try { return std::stoull(std::string(j.get_str(after_ofs, key))); } catch (...) {}
                        }
                        if (t == lite3cpp::Type::Bytes) {
                            auto bytes = j.get_bytes(after_ofs, key);
                            std::string s(reinterpret_cast<const char*>(bytes.data()), bytes.size());
                            try { return std::stoull(s); } catch (...) {}
                        }
                        return 0;
                    };
                    auto get_after_str = [&](std::string_view key, std::string_view def = "") -> std::string {
                        auto t = j.get_type(after_ofs, key);
                        if (t == lite3cpp::Type::String) return std::string(j.get_str(after_ofs, key));
                        if (t == lite3cpp::Type::Int64) return std::to_string(j.get_i64(after_ofs, key));
                        if (t == lite3cpp::Type::Bytes) {
                            auto bytes = j.get_bytes(after_ofs, key);
                            return std::string(reinterpret_cast<const char*>(bytes.data()), bytes.size());
                        }
                        return std::string(def);
                    };

                    uint64_t data_id = get_after_u64("data_id");
                    uint32_t repl_num = static_cast<uint32_t>(get_after_u64("data_repl_num"));
                    uint64_t resc_id = get_after_u64("resc_id");
                    uint64_t data_size = get_after_u64("data_size");
                    std::string checksum = get_after_str("data_checksum");
                    std::string modify_ts = get_after_str("modify_ts");

                    irods::catalog::replica repl;
                    repl.data_id = data_id;
                    repl.replica_number = repl_num;
                    repl.resource_id = resc_id;
                    repl.physical_path = get_after_str("data_path");
                    repl.resc_hier = get_after_str("resc_hier");
                    if (repl.resc_hier.empty()) {
                        repl.resc_hier = get_after_str("data_resc_hier");
                    }
                    if (repl.resc_hier.empty() && j.get_type(r_ofs, "file_modified") == lite3cpp::Type::Object) {
                        size_t fm_ofs = j.get_obj(r_ofs, "file_modified");
                        if (j.get_type(fm_ofs, "resc_hier") == lite3cpp::Type::String) {
                            repl.resc_hier = std::string(j.get_str(fm_ofs, "resc_hier"));
                        }
                        if (repl.resc_hier.empty() && j.get_type(fm_ofs, "dest_resc_hier") == lite3cpp::Type::String) {
                            repl.resc_hier = std::string(j.get_str(fm_ofs, "dest_resc_hier"));
                        }
                    }
                    if (repl.resc_hier.empty() && resc_id > 0) {
                        irods::catalog::snowflake_id_t rsid = g_catalog->make_id(irods::catalog::EntityType::Resource, resc_id);
                        auto rpayload = g_catalog->get_client()->get_node_payload_async(g_catalog->get_cluster_id(), rsid).get();
                        if (!rpayload.empty()) {
                            lite3cpp::Buffer rbuf(rpayload);
                            try { repl.resc_hier = rbuf.get_str(0, "n"); } catch (...) {}
                        }
                    }
                    repl.status = get_after_str("data_is_dirty", "1");
                    repl.checksum = checksum;
                    repl.modify_ts = get_timestamp(modify_ts);
                    repl.size = data_size;

                    // Update the replica in the catalog
                    g_catalog->register_replica(repl);

                    // Update data object size
                    g_catalog->modify_data_object(data_id, "DATA_SIZE", std::to_string(data_size));

                    std::string expiry = get_after_str("data_expiry_ts");
                    if (!expiry.empty()) {
                        g_catalog->modify_data_object(data_id, "ex", get_timestamp(expiry));
                    }
                }
            }
        }

        rodsLog(LOG_DEBUG, "L3_PLUGIN: EXITING db_data_object_finalize_op SUCCESS");
        return SUCCESS();
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_data_object_finalize_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

irods::error db_get_delay_rule_info_op(irods::plugin_context& _ctx, const char* _rule_id, std::vector<std::string>* _out_info) {
    rodsLog(LOG_DEBUG, "L3_PLUGIN: ENTERING db_get_delay_rule_info_op [%s]", safe_string(_rule_id).c_str());
    if (!_rule_id || !_out_info) return ERROR(SYS_INVALID_INPUT_PARAM, "null pointers");
    uint64_t id = 0;
    try { id = std::stoull(_rule_id); } catch (...) { return ERROR(SYS_INVALID_INPUT_PARAM, "invalid rule id"); }
    irods::catalog::rule_exec re;
    auto ret = g_catalog->get_rule_execution(id, re);
    if (!ret.ok()) {
        return ret;
    }
    _out_info->push_back(re.name);
    _out_info->push_back(re.rei_file_path);
    _out_info->push_back(re.user_name);
    _out_info->push_back(re.address);
    _out_info->push_back(re.exec_time);
    _out_info->push_back(re.frequency);
    _out_info->push_back(re.priority);
    _out_info->push_back(re.last_exec_time);
    _out_info->push_back(re.status);
    _out_info->push_back(re.estimate);
    _out_info->push_back(re.notification_addr);
    _out_info->push_back(re.context);
    rodsLog(LOG_DEBUG, "L3_PLUGIN: EXITING db_get_delay_rule_info_op SUCCESS");
    return SUCCESS();
}

irods::error db_delay_rule_lock_op(irods::plugin_context& _ctx, const char* _rule_id, const char* _lock_host, int _lock_host_pid) {
    rodsLog(LOG_DEBUG, "L3_PLUGIN: ENTERING db_delay_rule_lock_op rule [%s] host [%s] pid [%d]", safe_string(_rule_id).c_str(), safe_string(_lock_host).c_str(), _lock_host_pid);
    if (!_rule_id || !_lock_host) return ERROR(SYS_INTERNAL_NULL_INPUT_ERR, "null pointers");
    uint64_t id = 0;
    try { id = std::stoull(_rule_id); } catch (...) { return ERROR(SYS_INVALID_INPUT_PARAM, "invalid rule id"); }
    auto ret = g_catalog->lock_rule_execution(id, safe_string(_lock_host), _lock_host_pid);
    rodsLog(LOG_DEBUG, "L3_PLUGIN: EXITING db_delay_rule_lock_op ret=%d", ret.code());
    return ret;
}

irods::error db_delay_rule_unlock_op(irods::plugin_context& _ctx, const char* _rule_ids) {
    rodsLog(LOG_DEBUG, "L3_PLUGIN: ENTERING db_delay_rule_unlock_op [%s]", safe_string(_rule_ids).c_str());
    if (!_rule_ids) return ERROR(SYS_INTERNAL_NULL_INPUT_ERR, "null pointers");
    try {
        auto j = lite3cpp::lite3_json::from_json_string(_rule_ids);
        if (j.size() >= sizeof(lite3cpp::PackedNodeLayout)) {
            lite3cpp::NodeView nv(reinterpret_cast<const lite3cpp::PackedNodeLayout*>(j.data()));
            if (nv.type() == lite3cpp::Type::Array) {
                for (uint32_t i = 0; i < nv.size(); ++i) {
                    uint64_t id = 0;
                    auto t = j.arr_get_type(0, i);
                    if (t == lite3cpp::Type::String) {
                        try { id = std::stoull(std::string(j.arr_get_str(0, i))); } catch (...) {}
                    } else if (t == lite3cpp::Type::Int64) {
                        id = static_cast<uint64_t>(j.arr_get_i64(0, i));
                    }
                    if (id) g_catalog->unlock_rule_execution(id);
                }
            }
        }
    } catch (...) {}
    rodsLog(LOG_DEBUG, "L3_PLUGIN: EXITING db_delay_rule_unlock_op SUCCESS");
    return SUCCESS();
}

irods::error db_check_password_op(irods::plugin_context& _ctx, const char* _user, const char* _zone, const char* _password, int* _correct) {
    rodsLog(LOG_DEBUG, "L3_PLUGIN: ENTERING db_check_password_op user [%s] zone [%s]", safe_string(_user).c_str(), safe_string(_zone).c_str());
    if (_correct) *_correct = 0;
    rodsLog(LOG_DEBUG, "L3_PLUGIN: EXITING db_check_password_op SUCCESS");
    return SUCCESS();
}

irods::error db_make_session_token_op(irods::plugin_context& _ctx, const char* _user, const char* _zone, char* _token) {
    rodsLog(LOG_DEBUG, "L3_PLUGIN: ENTERING db_make_session_token_op user [%s] zone [%s]", safe_string(_user).c_str(), safe_string(_zone).c_str());
    rodsLog(LOG_DEBUG, "L3_PLUGIN: EXITING db_make_session_token_op SUCCESS");
    return SUCCESS();
}

irods::error db_check_session_token_op(irods::plugin_context& _ctx, const char* _user, const char* _zone, const char* _token, int* _correct) {
    rodsLog(LOG_DEBUG, "L3_PLUGIN: ENTERING db_check_session_token_op user [%s] zone [%s]", safe_string(_user).c_str(), safe_string(_zone).c_str());
    if (_correct) *_correct = 0;
    rodsLog(LOG_DEBUG, "L3_PLUGIN: EXITING db_check_session_token_op SUCCESS");
    return SUCCESS();
}

irods::error db_remove_session_tokens_op(irods::plugin_context& _ctx, const char* _user, const char* _zone) {
    rodsLog(LOG_DEBUG, "L3_PLUGIN: ENTERING db_remove_session_tokens_op user [%s] zone [%s]", safe_string(_user).c_str(), safe_string(_zone).c_str());
    rodsLog(LOG_DEBUG, "L3_PLUGIN: EXITING db_remove_session_tokens_op SUCCESS");
    return SUCCESS();
}

irods::error db_remove_password_op(irods::plugin_context& _ctx, const char* _user, const char* _zone) {
    rodsLog(LOG_DEBUG, "L3_PLUGIN: ENTERING db_remove_password_op user [%s] zone [%s]", safe_string(_user).c_str(), safe_string(_zone).c_str());
    rodsLog(LOG_DEBUG, "L3_PLUGIN: EXITING db_remove_password_op SUCCESS");
    return SUCCESS();
}

// Zones
irods::error db_reg_zone_op(irods::plugin_context& _ctx, const char* _zone, const char* _type, const char* _conn, const char* _comment) {
    try {
        rodsLog(LOG_DEBUG, "L3_PLUGIN: ENTERING db_reg_zone_op");
        irods::catalog::zone z;
        z.name = safe_string(_zone);
        z.type = safe_string(_type);
        z.connection = safe_string(_conn);
        z.comment = safe_string(_comment);
        auto ret = g_catalog->register_zone(z);
        rodsLog(LOG_DEBUG, "L3_PLUGIN: EXITING db_reg_zone_op SUCCESS");
        return ret;
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_reg_zone_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

irods::error db_mod_zone_op(irods::plugin_context& _ctx, const char* _zone, const char* _prop, const char* _val) {
    try {
        rodsLog(LOG_DEBUG, "L3_PLUGIN: ENTERING db_mod_zone_op");
        auto ret = g_catalog->modify_zone(safe_string(_zone), safe_string(_prop), safe_string(_val));
        rodsLog(LOG_DEBUG, "L3_PLUGIN: EXITING db_mod_zone_op SUCCESS");
        return ret;
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_mod_zone_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

irods::error db_del_zone_op(irods::plugin_context& _ctx, const char* _zone) {
    try {
        rodsLog(LOG_DEBUG, "L3_PLUGIN: ENTERING db_del_zone_op");
        auto ret = g_catalog->delete_zone(safe_string(_zone));
        rodsLog(LOG_DEBUG, "L3_PLUGIN: EXITING db_del_zone_op SUCCESS");
        return ret;
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_del_zone_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

// Token
irods::error db_reg_token_op(irods::plugin_context& _ctx, const char* _name, const char* _value, const char* _namespace) {
    try {
        rodsLog(LOG_DEBUG, "L3_PLUGIN: ENTERING db_reg_token_op");
        auto ret = g_catalog->register_token(safe_string(_name), safe_string(_value), safe_string(_namespace));
        rodsLog(LOG_DEBUG, "L3_PLUGIN: EXITING db_reg_token_op SUCCESS");
        return ret;
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_reg_token_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

irods::error db_del_token_op(irods::plugin_context& _ctx, const char* _name, const char* _namespace) {
    try {
        rodsLog(LOG_DEBUG, "L3_PLUGIN: ENTERING db_del_token_op");
        auto ret = g_catalog->delete_token(safe_string(_name), safe_string(_namespace));
        rodsLog(LOG_DEBUG, "L3_PLUGIN: EXITING db_del_token_op SUCCESS");
        return ret;
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_del_token_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

// Quota
irods::error db_set_quota_op(irods::plugin_context& _ctx, const char* _user, const char* _resc, rodsLong_t _limit) {
    try {
        rodsLog(LOG_DEBUG, "L3_PLUGIN: ENTERING db_set_quota_op");
        auto ret = g_catalog->set_quota(safe_string(_user), safe_string(_resc), _limit);
        rodsLog(LOG_DEBUG, "L3_PLUGIN: EXITING db_set_quota_op SUCCESS");
        return ret;
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_set_quota_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

irods::error db_check_quota_op(irods::plugin_context& _ctx, const char* _user, const char* _resc, rodsLong_t* _usage, int* _limit_exceeded) {
    try {
        rodsLog(LOG_DEBUG, "L3_PLUGIN: ENTERING db_check_quota_op");
        if (!_usage || !_limit_exceeded) return ERROR(SYS_INVALID_INPUT_PARAM, "Null inputs");
        int64_t usage = 0, limit = -1;
        auto ret = g_catalog->check_quota(safe_string(_user), safe_string(_resc), usage, limit);
        if (ret.ok()) { *_usage = usage; *_limit_exceeded = (limit >= 0 && usage > limit) ? 1 : 0; }
        rodsLog(LOG_DEBUG, "L3_PLUGIN: EXITING db_check_quota_op SUCCESS");
        return ret;
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_check_quota_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

irods::error db_calc_usage_and_quota_op(irods::plugin_context& _ctx) { return SUCCESS(); }

irods::error db_del_unused_avus_op(irods::plugin_context& _ctx) {
    rodsLog(LOG_DEBUG, "L3_PLUGIN: ENTERING db_del_unused_avus_op");
    return SUCCESS();
}

// Rules
irods::error db_reg_rule_exec_op(irods::plugin_context& _ctx, ruleExecSubmitInp_t* _info) {
    try {
        rodsLog(LOG_DEBUG, "L3_PLUGIN: ENTERING db_reg_rule_exec_op");
        if (!_info) return ERROR(SYS_INVALID_INPUT_PARAM, "null ruleExecSubmitInp_t");
        irods::catalog::rule_exec re;
        re.id = 0; 
        re.name = safe_string(_info->ruleName); 
        re.rei_file_path = safe_string(_info->reiFilePath);
        re.user_name = safe_string(_info->userName);
        re.address = safe_string(_info->exeAddress);
        re.exec_time = safe_string(_info->exeTime); 
        re.frequency = safe_string(_info->exeFrequency);
        re.priority = (_info->priority && _info->priority[0] != '\0') ? _info->priority : "5";
        re.last_exec_time = safe_string(_info->lastExecTime);
        re.status = safe_string(_info->exeStatus);
        re.estimate = safe_string(_info->estimateExeTime);
        re.notification_addr = safe_string(_info->notificationAddr);

        const char* ctx_val = getValByKey(&_info->condInput, RULE_EXECUTION_CONTEXT_KW);
        if (ctx_val) {
            re.context = ctx_val;
        }

        g_catalog->get_next_sequence_value("R_RULE_EXEC", re.id);
        uint64_t out_id = 0;
        auto ret = g_catalog->register_rule_execution(re, out_id);
        rstrcpy(_info->ruleExecId, std::to_string(re.id).c_str(), NAME_LEN);
        rodsLog(LOG_DEBUG, "L3_PLUGIN: EXITING db_reg_rule_exec_op SUCCESS (id=%s, prio=%s, exeTime=%s)", _info->ruleExecId, re.priority.c_str(), re.exec_time.c_str());
        return ret;
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_reg_rule_exec_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

irods::error db_del_rule_exec_op(irods::plugin_context& _ctx, const char* _rule_id) {
    try {
        rodsLog(LOG_DEBUG, "L3_PLUGIN: ENTERING db_del_rule_exec_op");
        std::string rid_str = safe_string(_rule_id);
        auto ret = g_catalog->delete_rule_execution(rid_str.empty() ? 0 : std::stoull(rid_str));
        rodsLog(LOG_DEBUG, "L3_PLUGIN: EXITING db_del_rule_exec_op SUCCESS");
        return ret;
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_del_rule_exec_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

// Specific Query
irods::error db_add_specific_query_op(irods::plugin_context& _ctx, const char* _alias, const char* _sql) {
    try {
        rodsLog(LOG_DEBUG, "L3_PLUGIN: ENTERING db_add_specific_query_op");
        auto ret = g_catalog->register_specific_query(safe_string(_alias), safe_string(_sql));
        rodsLog(LOG_DEBUG, "L3_PLUGIN: EXITING db_add_specific_query_op SUCCESS");
        return ret;
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_add_specific_query_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

irods::error db_del_specific_query_op(irods::plugin_context& _ctx, const char* _alias) {
    try {
        rodsLog(LOG_DEBUG, "L3_PLUGIN: ENTERING db_del_specific_query_op");
        auto ret = g_catalog->delete_specific_query(safe_string(_alias));
        rodsLog(LOG_DEBUG, "L3_PLUGIN: EXITING db_del_specific_query_op SUCCESS");
        return ret;
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_del_specific_query_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

irods::error db_specific_query_op(
    irods::plugin_context& _ctx,
    specificQueryInp_t*    _spec_query_inp,
    genQueryOut_t*         _result)
{
    try {
        rodsLog(LOG_DEBUG, "L3_PLUGIN: ENTERING db_specific_query_op");
        if (!_spec_query_inp || !_result) {
            return ERROR(CAT_INVALID_ARGUMENT, "null parameter in db_specific_query_op");
        }

        std::string sql_str = safe_string(_spec_query_inp->sql);
        rodsLog(LOG_DEBUG, "L3_PLUGIN: db_specific_query_op: query [%s]", sql_str.c_str());

        bool is_show_coll_acls = (sql_str == "ShowCollAcls" ||
                                  sql_str.find("ShowCollAcls") != std::string::npos ||
                                  (sql_str.find("R_COLL_MAIN") != std::string::npos && sql_str.find("R_OBJT_ACCESS") != std::string::npos));

        if (is_show_coll_acls) {
            bool has_query = false;
            g_catalog->has_specific_query("ShowCollAcls", has_query);
            if (!has_query) {
                rodsLog(LOG_DEBUG, "L3_PLUGIN: db_specific_query_op: ShowCollAcls not found in catalog");
                return ERROR(CAT_UNKNOWN_SPECIFIC_QUERY, "unknown query: ShowCollAcls");
            }

            std::string coll_name = safe_string(_spec_query_inp->args[0]);
            if (coll_name.empty()) {
                return ERROR(CAT_INVALID_ARGUMENT, "missing collection argument for ShowCollAcls");
            }

            std::vector<irods::catalog::AclEntry> acls;
            auto ret = g_catalog->get_collection_acls(coll_name, acls);
            if (!ret.ok()) {
                rodsLog(LOG_DEBUG, "L3_PLUGIN: db_specific_query_op: get_collection_acls failed: %s", ret.result().c_str());
                return ret;
            }

            _result->rowCnt = static_cast<int>(acls.size());
            _result->attriCnt = 4;
            _result->continueInx = 0;
            _result->totalRowCount = static_cast<int>(acls.size());

            int col_len = 2700;
            for (int i = 0; i < 4; ++i) {
                _result->sqlResult[i].attriInx = i;
                _result->sqlResult[i].len = col_len;
                _result->sqlResult[i].value = static_cast<char*>(malloc(_result->rowCnt * col_len));
                if (!_result->sqlResult[i].value) {
                    return ERROR(SYS_MALLOC_ERR, "failed to allocate memory for specific query results");
                }
                std::memset(_result->sqlResult[i].value, 0, _result->rowCnt * col_len);
            }

            for (int r = 0; r < _result->rowCnt; ++r) {
                const auto& entry = acls[r];
                std::strncpy(&_result->sqlResult[0].value[r * col_len], entry.user_name.c_str(), col_len - 1);
                std::strncpy(&_result->sqlResult[1].value[r * col_len], entry.zone_name.c_str(), col_len - 1);
                std::strncpy(&_result->sqlResult[2].value[r * col_len], entry.access_name.c_str(), col_len - 1);
                std::strncpy(&_result->sqlResult[3].value[r * col_len], entry.user_type.c_str(), col_len - 1);
            }

            rodsLog(LOG_DEBUG, "L3_PLUGIN: EXITING db_specific_query_op SUCCESS (%d rows)", _result->rowCnt);
            return SUCCESS();
        }

        // Check if query exists in catalog
        bool has_query = false;
        g_catalog->has_specific_query(sql_str, has_query);
        if (!has_query) {
            rodsLog(LOG_DEBUG, "L3_PLUGIN: db_specific_query_op: unknown query [%s]", sql_str.c_str());
            return ERROR(CAT_UNKNOWN_SPECIFIC_QUERY, "unknown query: " + sql_str);
        }

        // Default fallback for other registered specific queries: 0 rows
        _result->rowCnt = 0;
        _result->attriCnt = 0;
        _result->continueInx = 0;
        _result->totalRowCount = 0;
        rodsLog(LOG_DEBUG, "L3_PLUGIN: EXITING db_specific_query_op (0 rows)");
        return ERROR(CAT_NO_ROWS_FOUND, "CAT_NO_ROWS_FOUND");
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_specific_query_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

// Logical Quota
irods::error db_set_logical_quota_op(irods::plugin_context& _ctx, const char* _coll_name, rodsLong_t _limit) {
    try {
        rodsLog(LOG_DEBUG, "L3_PLUGIN: ENTERING db_set_logical_quota_op");
        auto ret = g_catalog->set_logical_quota(safe_string(_coll_name), _limit);
        rodsLog(LOG_DEBUG, "L3_PLUGIN: EXITING db_set_logical_quota_op SUCCESS");
        return ret;
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_set_logical_quota_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

irods::error db_check_logical_quota_op(irods::plugin_context& _ctx, const char* _coll_name, std::vector<std::tuple<std::string, std::int64_t, std::int64_t, std::int64_t, std::int64_t>>* _quota_values) {
    try {
        rodsLog(LOG_DEBUG, "L3_PLUGIN: ENTERING db_check_logical_quota_op");
        if (!_coll_name || !_quota_values) return ERROR(SYS_INVALID_INPUT_PARAM, "Null inputs");
        int64_t usage = 0, limit = -1;
        auto ret = g_catalog->check_logical_quota(safe_string(_coll_name), usage, limit);
        if (ret.ok()) _quota_values->push_back(std::make_tuple(safe_string(_coll_name), usage, limit, 0, 0));
        rodsLog(LOG_DEBUG, "L3_PLUGIN: EXITING db_check_logical_quota_op SUCCESS");
        return ret;
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_check_logical_quota_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

irods::error db_calc_logical_usage_and_quota_op(irods::plugin_context& _ctx, const char* _coll_name, rodsLong_t* _usage, rodsLong_t* _limit) {
    try {
        rodsLog(LOG_DEBUG, "L3_PLUGIN: ENTERING db_calc_logical_usage_and_quota_op");
        int64_t usage = 0, limit = -1;
        auto ret = g_catalog->calculate_logical_usage(safe_string(_coll_name), usage);
        if (ret.ok()) ret = g_catalog->check_logical_quota(safe_string(_coll_name), usage, limit);
        if (_usage) *_usage = usage; if (_limit) *_limit = limit;
        rodsLog(LOG_DEBUG, "L3_PLUGIN: EXITING db_calc_logical_usage_and_quota_op SUCCESS");
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
        rodsLog(LOG_DEBUG, "L3_PLUGIN: ENTERING db_set_grid_configuration_value_op");
        if (auto ret = init_l3kvg_catalog(); !ret.ok()) return ret;
        std::string full_name = safe_string(_ns) + ":" + safe_string(_name);
        auto ret = g_catalog->set_grid_configuration_value(full_name, safe_string(_value));
        rodsLog(LOG_DEBUG, "L3_PLUGIN: EXITING db_set_grid_configuration_value_op SUCCESS");
        return ret;
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_set_grid_configuration_value_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

irods::error db_get_grid_configuration_value_op(irods::plugin_context& _ctx, const char* _ns, const char* _name, char* _value, std::size_t _value_buf_size) {
    try {
        rodsLog(LOG_DEBUG, "L3_PLUGIN: ENTERING db_get_grid_configuration_value_op");
        if (auto ret = init_l3kvg_catalog(); !ret.ok()) return ret;
        std::string val;
        std::string full_name = safe_string(_ns) + ":" + safe_string(_name);
        auto ret = g_catalog->get_grid_configuration_value(full_name, val);
        if (ret.ok()) strncpy(_value, val.c_str(), _value_buf_size - 1);
        rodsLog(LOG_DEBUG, "L3_PLUGIN: EXITING db_get_grid_configuration_value_op SUCCESS");
        return ret;
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_get_grid_configuration_value_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

namespace irods::catalog::bridge {
    irods::experimental::genquery2::select synthesize_gq2_ast(genQueryInp_t* _inp, irods::catalog::CatalogFacade* _catalog, std::vector<uint64_t>& _starting_nodes);
    void pack_gq1_results(const irods::catalog::ResultSet& _results, genQueryInp_t* _inp, genQueryOut_t* _out);
    int get_pure_inx(int inx);
}

// GenQuery
irods::error db_get_catalog_version_op(irods::plugin_context& _ctx, int* _version) {
    rodsLog(LOG_DEBUG, "L3_PLUGIN: ENTERING db_get_catalog_version_op");
    try {
        if (!_version) return ERROR(SYS_INVALID_INPUT_PARAM, "Null version pointer");
        *_version = 2; 
        rodsLog(LOG_DEBUG, "L3_PLUGIN: EXITING db_get_catalog_version_op SUCCESS");
        return SUCCESS();

    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_get_catalog_version_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

irods::error db_initialize_catalog_op(irods::plugin_context& _ctx) {
    try {
        rodsLog(LOG_DEBUG, "L3_PLUGIN: ENTERING db_initialize_catalog_op");
        if (auto ret = init_l3kvg_catalog(); !ret.ok()) return ret;
        const auto& config = irods::server_properties::instance().map().get_json();
        auto ret = g_catalog->bootstrap_catalog(config.at(KW_CFG_ZONE_NAME).get<std::string>(), config.at(KW_CFG_ZONE_USER).get<std::string>());
        rodsLog(LOG_DEBUG, "L3_PLUGIN: EXITING db_initialize_catalog_op SUCCESS");
        return ret;
    } catch(const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_initialize_catalog_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

thread_local std::string g_access_control_user_name;
thread_local std::string g_access_control_zone;
thread_local int g_access_control_priv = 0;
thread_local int g_access_control_flag = 0;

irods::error db_gen_query_access_control_setup_op(
    irods::plugin_context& _ctx,
    const char* _user,
    const char* _zone,
    const char* _host,
    int _priv,
    int _control_flag)
{
    if (_user) {
        g_access_control_user_name = safe_string(_user);
        g_access_control_zone = safe_string(_zone);
        g_access_control_priv = _priv;
    }
    int old_flag = g_access_control_flag;
    if (_control_flag >= 0) {
        g_access_control_flag = _control_flag;
    }
    return CODE(old_flag);
}

irods::error db_gen_query_op(irods::plugin_context& _ctx, genQueryInp_t* _inp, genQueryOut_t* _out) {
    try {
        rodsLog(LOG_DEBUG, "L3_PLUGIN: ENTERING db_gen_query_op");
        if (auto ret = init_l3kvg_catalog(); !ret.ok()) return ret;
        if (!_inp || !_out) return ERROR(SYS_INTERNAL_NULL_INPUT_ERR, "Null input/output");

        // 1. Intercept ticket restriction queries
        bool is_host = false, is_user = false, is_group = false;
        for (int s = 0; s < _inp->selectInp.len; ++s) {
            int sinx = _inp->selectInp.inx[s];
            if (sinx == COL_TICKET_ALLOWED_HOST || sinx == COL_TICKET_ALLOWED_HOST_TICKET_ID) is_host = true;
            if (sinx == COL_TICKET_ALLOWED_USER_NAME || sinx == COL_TICKET_ALLOWED_USER_TICKET_ID) is_user = true;
            if (sinx == COL_TICKET_ALLOWED_GROUP_NAME || sinx == COL_TICKET_ALLOWED_GROUP_TICKET_ID) is_group = true;
        }

        if (is_host || is_user || is_group) {
            std::string ticket_id_or_str;
            for (int c = 0; c < _inp->sqlCondInp.len; ++c) {
                int cinx = _inp->sqlCondInp.inx[c];
                if (cinx == COL_TICKET_ALLOWED_HOST_TICKET_ID ||
                    cinx == COL_TICKET_ALLOWED_USER_TICKET_ID ||
                    cinx == COL_TICKET_ALLOWED_GROUP_TICKET_ID ||
                    cinx == COL_TICKET_ID || cinx == COL_TICKET_STRING) {
                    std::string cond_str = safe_string(_inp->sqlCondInp.value[c]);
                    size_t first = cond_str.find_first_not_of("= '\"");
                    size_t last = cond_str.find_last_not_of(" '\"");
                    if (first != std::string::npos && last != std::string::npos && last >= first) {
                        ticket_id_or_str = cond_str.substr(first, last - first + 1);
                    }
                }
            }

            std::vector<std::pair<std::string, std::string>> out_restrictions;
            std::string restr_type = is_host ? "host" : (is_user ? "user" : "group");
            g_catalog->get_ticket_restrictions(ticket_id_or_str, restr_type, out_restrictions);
            if (out_restrictions.empty()) {
                return ERROR(CAT_NO_ROWS_FOUND, "No rows found");
            }

            _out->rowCnt = out_restrictions.size();
            _out->attriCnt = _inp->selectInp.len;
            _out->continueInx = 0;
            _out->totalRowCount = _out->rowCnt;
            for (int s = 0; s < _inp->selectInp.len; ++s) {
                int sinx = _inp->selectInp.inx[s];
                int col_len = 1000;
                _out->sqlResult[s].attriInx = sinx;
                _out->sqlResult[s].len = col_len;
                _out->sqlResult[s].value = (char*)malloc(_out->rowCnt * col_len);
                memset(_out->sqlResult[s].value, 0, _out->rowCnt * col_len);
                for (size_t r = 0; r < out_restrictions.size(); ++r) {
                    char* dst = _out->sqlResult[s].value + r * col_len;
                    if (sinx == COL_TICKET_ALLOWED_HOST_TICKET_ID ||
                        sinx == COL_TICKET_ALLOWED_USER_TICKET_ID ||
                        sinx == COL_TICKET_ALLOWED_GROUP_TICKET_ID) {
                        strncpy(dst, out_restrictions[r].first.c_str(), col_len - 1);
                    } else {
                        strncpy(dst, out_restrictions[r].second.c_str(), col_len - 1);
                    }
                }
            }
            return SUCCESS();
        }

        // 2. Setup options & validate ticket if session ticket is active
        irods::experimental::genquery2::options opts;
        std::string ticket_target_path;
        std::string ticket_target_type;
        std::string ticket_cuser;
        bool is_caller_admin = (_ctx.comm() ? (_ctx.comm()->clientUser.authInfo.authFlag >= LOCAL_PRIV_USER_AUTH) : (g_access_control_priv >= LOCAL_PRIV_USER_AUTH));
        if (g_session_ticket[0] != '\0') {
            if (_ctx.comm() && _ctx.comm()->clientUser.userName[0] != '\0') {
                ticket_cuser = _ctx.comm()->clientUser.userName;
            } else if (g_session_client_user[0] != '\0') {
                ticket_cuser = g_session_client_user;
            } else if (!g_access_control_user_name.empty()) {
                ticket_cuser = g_access_control_user_name;
            }
            auto terr = g_catalog->validate_ticket(g_session_ticket, ticket_cuser, g_session_client_addr, &ticket_target_path, &ticket_target_type);
            if (!terr.ok()) {
                return terr;
            }
            opts.admin_mode = true;
        } else {
            if (_ctx.comm()) {
                opts.user_name = _ctx.comm()->clientUser.userName;
                opts.user_zone = _ctx.comm()->clientUser.rodsZone;
                opts.admin_mode = (_ctx.comm()->clientUser.authInfo.authFlag >= LOCAL_PRIV_USER_AUTH);
            } else if (!g_access_control_user_name.empty()) {
                opts.user_name = g_access_control_user_name;
                opts.user_zone = g_access_control_zone;
                opts.admin_mode = (g_access_control_priv >= LOCAL_PRIV_USER_AUTH);
            }
        }
        if (_inp->maxRows > 0) {
            opts.default_number_of_rows = _inp->maxRows;
        }

        std::vector<uint64_t> starting_nodes;
        auto ast = irods::catalog::bridge::synthesize_gq2_ast(_inp, g_catalog.get(), starting_nodes);
        irods::catalog::ResultSet results;
        auto ret = g_catalog->execute_query(ast, results, starting_nodes, "", &opts);

        if (g_session_ticket[0] != '\0' && !is_caller_admin && !ticket_target_path.empty()) {
            int coll_name_idx = -1;
            int data_name_idx = -1;
            for (int i = 0; i < _inp->selectInp.len; ++i) {
                int pinx = irods::catalog::bridge::get_pure_inx(_inp->selectInp.inx[i]);
                if (pinx == COL_COLL_NAME) {
                    coll_name_idx = i;
                } else if (pinx == COL_DATA_NAME) {
                    data_name_idx = i;
                }
            }

            std::string cond_coll_name;
            std::string cond_data_name;
            for (int k = 0; k < _inp->sqlCondInp.len; ++k) {
                int pinx = irods::catalog::bridge::get_pure_inx(_inp->sqlCondInp.inx[k]);
                if (pinx == COL_COLL_NAME && _inp->sqlCondInp.value[k]) {
                    std::string val = _inp->sqlCondInp.value[k];
                    auto p1 = val.find('\'');
                    if (p1 != std::string::npos) {
                        auto p2 = val.rfind('\'');
                        if (p2 != std::string::npos && p2 > p1) {
                            cond_coll_name = val.substr(p1 + 1, p2 - p1 - 1);
                        }
                    }
                } else if (pinx == COL_DATA_NAME && _inp->sqlCondInp.value[k]) {
                    std::string val = _inp->sqlCondInp.value[k];
                    auto p1 = val.find('\'');
                    if (p1 != std::string::npos) {
                        auto p2 = val.rfind('\'');
                        if (p2 != std::string::npos && p2 > p1) {
                            cond_data_name = val.substr(p1 + 1, p2 - p1 - 1);
                        }
                    }
                }
            }

            if (coll_name_idx >= 0 || data_name_idx >= 0 || !cond_coll_name.empty()) {
                std::vector<l3kvg::Query::ResultRow> filtered_rows;
                for (size_t r = 0; r < results.rows.size(); ++r) {
                    std::string cname = (coll_name_idx >= 0) ? std::string(results.get_field(r, (size_t)coll_name_idx)) : cond_coll_name;
                    std::string dname = (data_name_idx >= 0) ? std::string(results.get_field(r, (size_t)data_name_idx)) : cond_data_name;
                    std::string row_path;
                    bool is_coll_entity = false;
                    if (!dname.empty()) {
                        row_path = cname.empty() ? dname : (cname + "/" + dname);
                    } else if (!cname.empty()) {
                        row_path = cname;
                        is_coll_entity = true;
                    }

                    bool allowed = false;
                    if (!row_path.empty()) {
                        if (ticket_target_type == "data") {
                            if (row_path == ticket_target_path) allowed = true;
                        } else {
                            if (row_path == ticket_target_path || row_path.starts_with(ticket_target_path == "/" ? "/" : ticket_target_path + "/")) {
                                allowed = true;
                            }
                        }
                        if (!allowed) {
                            irods::catalog::snowflake_id_t u_sid = 0;
                            irods::catalog::snowflake_id_t obj_sid = 0;
                            auto e_type = is_coll_entity ? irods::catalog::EntityType::Collection : irods::catalog::EntityType::DataObject;
                            if (g_catalog->resolve_user_name(ticket_cuser, u_sid).ok() &&
                                g_catalog->resolve_path(row_path, obj_sid, e_type).ok() && obj_sid != 0) {
                                bool perm_allowed = false;
                                if (g_catalog->check_permission(u_sid, obj_sid, "read", perm_allowed).ok() && perm_allowed) {
                                    allowed = true;
                                }
                            }
                        }
                    } else {
                        allowed = true;
                    }

                    if (allowed) {
                        filtered_rows.push_back(std::move(results.rows[r]));
                    }
                }
                results.rows = std::move(filtered_rows);
            }
        }

        irods::catalog::bridge::pack_gq1_results(results, _inp, _out);
        if (_out->rowCnt <= 0) {
            rodsLog(LOG_DEBUG, "L3_PLUGIN: EXITING db_gen_query_op SUCCESS (NO ROWS)");
            return ERROR(CAT_NO_ROWS_FOUND, "No rows found");
        }

        // 3. If session ticket is active and data object was queried during an access check, increment uses
        if (g_session_ticket[0] != '\0') {
            bool is_access_check = false;
            if (_inp && _inp->condInput.len > 0) {
                for (int k = 0; k < _inp->condInput.len; ++k) {
                    if (_inp->condInput.keyWord[k] && 
                        (strcmp(_inp->condInput.keyWord[k], ACCESS_PERMISSION_KW) == 0 ||
                         strcmp(_inp->condInput.keyWord[k], TICKET_KW) == 0)) {
                        is_access_check = true;
                        break;
                    }
                }
            }
            if (is_access_check) {
                uint64_t q_data_id = 0;
                for (int i = 0; i < _out->attriCnt; ++i) {
                    if (_out->sqlResult[i].attriInx == COL_D_DATA_ID) {
                        try {
                            q_data_id = std::stoull(_out->sqlResult[i].value);
                        } catch (...) {}
                        break;
                    }
                }
                if (q_data_id != 0) {
                    auto uerr = g_catalog->increment_ticket_uses(g_session_ticket, q_data_id);
                    if (!uerr.ok()) {
                        return uerr;
                    }
                }
            }
        }

        rodsLog(LOG_DEBUG, "L3_PLUGIN: EXITING db_gen_query_op SUCCESS");
        return SUCCESS();
    } catch (const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: EXITING db_gen_query_op EXCEPTION: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
}

irods::error db_execute_genquery2_op(
    irods::plugin_context& _ctx,
    const irods::experimental::genquery2::statement* _stmt,
    const irods::experimental::genquery2::options* _opts,
    char** _output)
{
    if (!_stmt || !_opts || !_output) {
        return ERROR(SYS_INTERNAL_NULL_INPUT_ERR, "Null input pointers.");
    }

    *_output = nullptr;

    if (auto err = init_l3kvg_catalog(); !err.ok()) {
        return err;
    }

    if (!g_catalog) {
        return ERROR(SYS_CONFIG_FILE_ERR, "Catalog facade not initialized.");
    }

    try {
        if (const auto* sel = std::get_if<irods::experimental::genquery2::select>(_stmt)) {
            irods::catalog::ResultSet results;
            std::vector<uint64_t> starting_nodes;
            auto ret = g_catalog->execute_query(*sel, results, starting_nodes, "", _opts);
            if (!ret.ok()) {
                if (ret.code() == SYS_INVALID_INPUT_PARAM && _ctx.comm()) {
                    addRErrorMsg(&_ctx.comm()->rError, SYS_INVALID_INPUT_PARAM, "Could not generate SQL from GenQuery2 string.");
                }
                return ret;
            }

            std::stringstream ss;
            ss << "[";
            for (size_t r = 0; r < results.rows.size(); ++r) {
                if (r > 0) ss << ",";
                ss << "[";
                for (size_t c = 0; c < sel->projections.size(); ++c) {
                    if (c > 0) ss << ",";
                    std::string_view val = results.get_field(r, c);
                    ss << "\"";
                    for (char ch : val) {
                        switch (ch) {
                            case '"': ss << "\\\""; break;
                            case '\\': ss << "\\\\"; break;
                            case '\b': ss << "\\b"; break;
                            case '\f': ss << "\\f"; break;
                            case '\n': ss << "\\n"; break;
                            case '\r': ss << "\\r"; break;
                            case '\t': ss << "\\t"; break;
                            default:
                                if (static_cast<unsigned char>(ch) < 0x20) {
                                    char hex[8];
                                    std::snprintf(hex, sizeof(hex), "\\u%04x", static_cast<unsigned char>(ch));
                                    ss << hex;
                                } else {
                                    ss << ch;
                                }
                                break;
                        }
                    }
                    ss << "\"";
                }
                ss << "]";
            }
            ss << "]";

            std::string out_str = ss.str();
            *_output = strdup(out_str.c_str());
            if (!*_output) {
                return ERROR(SYS_MALLOC_ERR, "Failed to allocate memory for GenQuery2 output.");
            }
            return SUCCESS();
        }

        // Handle DML Mutations
        irods::catalog::compiler::Gq2ToL3kvgCompiler compiler;
        auto plan = compiler.compile(*_stmt);
        lite3cpp::Buffer dml_result;
        auto ret = g_catalog->execute_dml(plan, dml_result);
        if (!ret.ok()) {
            return ret;
        }

        std::string out_str = lite3cpp::lite3_json::to_json_string(dml_result, 0);
        *_output = strdup(out_str.c_str());
        if (!*_output) {
            return ERROR(SYS_MALLOC_ERR, "Failed to allocate memory for GenQuery2 output.");
        }
        return SUCCESS();
    }
    catch (const std::invalid_argument& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: db_execute_genquery2_op invalid argument: %s", e.what());
        if (_ctx.comm()) {
            addRErrorMsg(&_ctx.comm()->rError, SYS_INVALID_INPUT_PARAM, "Could not generate SQL from GenQuery2 string.");
        }
        return ERROR(SYS_INVALID_INPUT_PARAM, e.what());
    }
    catch (const irods::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: db_execute_genquery2_op irods::exception: %s", e.what());
        return ERROR(e.code(), e.what());
    }
    catch (const std::exception& e) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: db_execute_genquery2_op exception: %s", e.what());
        return ERROR(SYS_INTERNAL_ERR, e.what());
    }
    catch (...) {
        rodsLog(LOG_ERROR, "L3_PLUGIN: db_execute_genquery2_op unknown exception");
        return ERROR(SYS_UNKNOWN_ERROR, "Unknown exception in db_execute_genquery2_op");
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
        add_operation<collInfo_t*>(irods::DATABASE_OP_REG_COLL_BY_ADMIN, std::function<irods::error(irods::plugin_context&, collInfo_t*)>(db_reg_coll_by_admin_op));
        add_operation<collInfo_t*>(irods::DATABASE_OP_MOD_COLL, std::function<irods::error(irods::plugin_context&, collInfo_t*)>(db_mod_coll_op));
        add_operation<collInfo_t*>(irods::DATABASE_OP_DEL_COLL, std::function<irods::error(irods::plugin_context&, collInfo_t*)>(db_del_coll_op));
        add_operation<collInfo_t*>(irods::DATABASE_OP_DEL_COLL_BY_ADMIN, std::function<irods::error(irods::plugin_context&, collInfo_t*)>(db_del_coll_by_admin_op));
        add_operation<const char*, const char*>(irods::DATABASE_OP_RENAME_COLL, std::function<irods::error(irods::plugin_context&, const char*, const char*)>(db_rename_coll_op));

        add_operation<std::map<std::string, std::string>*>(irods::DATABASE_OP_REG_RESC, std::function<irods::error(irods::plugin_context&, std::map<std::string, std::string>*)>(db_reg_resc_op));
        add_operation<const char*, const char*, const char*>(irods::DATABASE_OP_MOD_RESC, std::function<irods::error(irods::plugin_context&, const char*, const char*, const char*)>(db_mod_resc_op));
        add_operation<const char*, const char*, const char*, const char*>(irods::DATABASE_OP_MOD_RESC_DATA_PATHS, std::function<irods::error(irods::plugin_context&, const char*, const char*, const char*, const char*)>(db_mod_resc_data_paths_op));
        add_operation<const char*, const char*>(irods::DATABASE_OP_MOD_RESC_FREESPACE, std::function<irods::error(irods::plugin_context&, const char*, const char*)>(db_mod_resc_freespace_op));
        add_operation<const char*, int>(irods::DATABASE_OP_DEL_RESC, std::function<irods::error(irods::plugin_context&, const char*, int)>(db_del_resc_op));
        add_operation<std::map<std::string, std::string>*>(irods::DATABASE_OP_ADD_CHILD_RESC, std::function<irods::error(irods::plugin_context&, std::map<std::string, std::string>*)>(db_add_child_resc_op));
        add_operation<std::map<std::string, std::string>*>(irods::DATABASE_OP_DEL_CHILD_RESC, std::function<irods::error(irods::plugin_context&, std::map<std::string, std::string>*)>(db_del_child_resc_op));
        add_operation<const char*, char**>(irods::DATABASE_OP_GET_HIERARCHY_FOR_RESC, std::function<irods::error(irods::plugin_context&, const char*, char**)>(db_get_hierarchy_for_resc_op));

        add_operation<userInfo_t*>(irods::DATABASE_OP_REG_USER_RE, std::function<irods::error(irods::plugin_context&, userInfo_t*)>(db_reg_user_re_op));
        add_operation<userInfo_t*>(irods::DATABASE_OP_DEL_USER_RE, std::function<irods::error(irods::plugin_context&, userInfo_t*)>(db_del_user_re_op));
        add_operation<const char*, const char*, const char*>(irods::DATABASE_OP_MOD_USER, std::function<irods::error(irods::plugin_context&, const char*, const char*, const char*)>(db_mod_user_op));
        add_operation<const char*, const char*, const char*, const char*, int*, int*>(irods::DATABASE_OP_CHECK_AUTH, std::function<irods::error(irods::plugin_context&, const char*, const char*, const char*, const char*, int*, int*)>(db_check_auth_op));
        add_operation<const char*, const char*, const char*, int*>(irods::DATABASE_OP_CHECK_AUTH_CREDENTIALS, std::function<irods::error(irods::plugin_context&, const char*, const char*, const char*, int*)>(db_check_auth_credentials_op));
        add_operation<const char*, int, const char*, char**, std::size_t>(irods::DATABASE_OP_UPDATE_PAM_PASSWORD, std::function<irods::error(irods::plugin_context&, const char*, int, const char*, char**, std::size_t)>(db_update_pam_password_op));
        add_operation<char*, const char*>(irods::DATABASE_OP_MAKE_TEMP_PW, std::function<irods::error(irods::plugin_context&, char*, const char*)>(db_make_temp_pw_op));
        add_operation<int, char*>(irods::DATABASE_OP_MAKE_LIMITED_PW, std::function<irods::error(irods::plugin_context&, int, char*)>(db_make_limited_pw_op));
        add_operation<const char*, const char*, const char*, const char*>(irods::DATABASE_OP_MOD_GROUP, std::function<irods::error(irods::plugin_context&, const char*, const char*, const char*, const char*)>(db_mod_group_op));

        add_operation<const char*, const char*, const char*, const char*, const char*, const KeyValPair*>(irods::DATABASE_OP_SET_AVU_METADATA, std::function<irods::error(irods::plugin_context&, const char*, const char*, const char*, const char*, const char*, const KeyValPair*)>(db_set_avu_metadata_op));
        add_operation<const char*, const char*, const char*, const char*, const char*, const KeyValPair*>(irods::DATABASE_OP_ADD_AVU_METADATA, std::function<irods::error(irods::plugin_context&, const char*, const char*, const char*, const char*, const char*, const KeyValPair*)>(db_add_avu_metadata_op));
        add_operation<int, const char*, const char*, const char*, const char*, const char*, int, const KeyValPair*>(irods::DATABASE_OP_DEL_AVU_METADATA, std::function<irods::error(irods::plugin_context&, int, const char*, const char*, const char*, const char*, const char*, int, const KeyValPair*)>(db_del_avu_metadata_op));
        add_operation<const char*, const char*, const char*, const char*, const char*, const char*, const char*, const char*, const KeyValPair*>(irods::DATABASE_OP_MOD_AVU_METADATA, std::function<irods::error(irods::plugin_context&, const char*, const char*, const char*, const char*, const char*, const char*, const char*, const char*, const KeyValPair*)>(db_mod_avu_metadata_op));
        add_operation<const char*, const char*, const char*, const char*, const KeyValPair*>(irods::DATABASE_OP_COPY_AVU_METADATA, std::function<irods::error(irods::plugin_context&, const char*, const char*, const char*, const char*, const KeyValPair*)>(db_copy_avu_metadata_op));
        
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
        add_operation<specificQueryInp_t*, genQueryOut_t*>(irods::DATABASE_OP_SPECIFIC_QUERY, std::function<irods::error(irods::plugin_context&, specificQueryInp_t*, genQueryOut_t*)>(db_specific_query_op));

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
        add_operation<const char*, const char*, const char*, const char*, const char*, const KeyValPair*>(
            irods::DATABASE_OP_MOD_TICKET,
            std::function<irods::error(irods::plugin_context&, const char*, const char*, const char*, const char*, const char*, const KeyValPair*)>(db_mod_ticket_op));
        add_operation<const char*>(irods::DATABASE_OP_DATA_OBJECT_FINALIZE, std::function<irods::error(irods::plugin_context&, const char*)>(db_data_object_finalize_op));
        add_operation(irods::DATABASE_OP_DEL_UNUSED_AVUS, std::function<irods::error(irods::plugin_context&)>(db_del_unused_avus_op));
        add_operation<const char*, std::vector<std::string>*>(irods::DATABASE_OP_GET_DELAY_RULE_INFO, std::function<irods::error(irods::plugin_context&, const char*, std::vector<std::string>*)>(db_get_delay_rule_info_op));
        add_operation<const char*, const char*, int>(irods::DATABASE_OP_DELAY_RULE_LOCK, std::function<irods::error(irods::plugin_context&, const char*, const char*, int)>(db_delay_rule_lock_op));
        add_operation<const char*>(irods::DATABASE_OP_DELAY_RULE_UNLOCK, std::function<irods::error(irods::plugin_context&, const char*)>(db_delay_rule_unlock_op));
        add_operation<const char*, const char*, const char*, int*>(irods::DATABASE_OP_CHECK_PASSWORD, std::function<irods::error(irods::plugin_context&, const char*, const char*, const char*, int*)>(db_check_password_op));
        add_operation<const char*, const char*, char*>(irods::DATABASE_OP_MAKE_SESSION_TOKEN, std::function<irods::error(irods::plugin_context&, const char*, const char*, char*)>(db_make_session_token_op));
        add_operation<const char*, const char*, const char*, int*>(irods::DATABASE_OP_CHECK_SESSION_TOKEN, std::function<irods::error(irods::plugin_context&, const char*, const char*, const char*, int*)>(db_check_session_token_op));
        add_operation<const char*, const char*>(irods::DATABASE_OP_REMOVE_SESSION_TOKENS, std::function<irods::error(irods::plugin_context&, const char*, const char*)>(db_remove_session_tokens_op));
        add_operation<const char*, const char*>(irods::DATABASE_OP_REMOVE_PASSWORD, std::function<irods::error(irods::plugin_context&, const char*, const char*)>(db_remove_password_op));

        add_operation<int*>("database_get_catalog_version", std::function<irods::error(irods::plugin_context&, int*)>(db_get_catalog_version_op));
        add_operation("database_initialize_catalog", std::function<irods::error(irods::plugin_context&)>(db_initialize_catalog_op));
        add_operation<genQueryInp_t*, genQueryOut_t*>(irods::DATABASE_OP_GEN_QUERY, std::function<irods::error(irods::plugin_context&, genQueryInp_t*, genQueryOut_t*)>(db_gen_query_op));
        add_operation<const char*, const char*, const char*, int, int>(
            irods::DATABASE_OP_GEN_QUERY_ACCESS_CONTROL_SETUP,
            std::function<irods::error(irods::plugin_context&, const char*, const char*, const char*, int, int)>(db_gen_query_access_control_setup_op));
        add_operation<const char*, const char*>(
            irods::DATABASE_OP_GEN_QUERY_TICKET_SETUP,
            std::function<irods::error(irods::plugin_context&, const char*, const char*)>(db_gen_query_ticket_setup_op));
        add_operation<const irods::experimental::genquery2::statement*,
                      const irods::experimental::genquery2::options*,
                      char**>(
            irods::DATABASE_OP_EXECUTE_GENQUERY2,
            std::function<irods::error(irods::plugin_context&,
                                       const irods::experimental::genquery2::statement*,
                                       const irods::experimental::genquery2::options*,
                                       char**)>(db_execute_genquery2_op));
    }
};

extern "C" irods::database* plugin_factory(const std::string& _inst_name, const std::string& _context) { return new l3kvg_database_plugin(_inst_name, _context); }
