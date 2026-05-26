#include "irods/rodsGenQuery.h"
#include "irods/catalog/gq2_compiler.hpp"
#include "irods/catalog/catalog_facade.hpp"
#include "irods/rodsLog.h"
#include "irods/filesystem/path.hpp"
#include <vector>
#include <string>
#include <map>
#include <regex>
#include <cstring>
#include <iostream>

namespace irods::catalog::bridge {

    /**
     * Synthesizes a GenQuery2 AST from a legacy GenQuery1 input.
     */
    irods::experimental::genquery2::select synthesize_gq2_ast(genQueryInp_t* _inp, CatalogFacade* _catalog, std::vector<uint64_t>& _starting_nodes) {
        rodsLog(LOG_NOTICE, "L3_BRIDGE: Synthesizing GQ2 AST from GQ1 Input. Projections: %d, Conditions: %d", _inp->selectInp.len, _inp->sqlCondInp.len);
        
        namespace gq2 = irods::experimental::genquery2;
        gq2::select ast;

        auto get_col_name = [](int pure_inx) -> std::string {
            switch(pure_inx) {
                case COL_D_DATA_ID: return "DATA_ID";
                case COL_D_COLL_ID: return "DATA_COLL_ID";
                case COL_DATA_NAME: return "DATA_NAME";
                case COL_DATA_REPL_NUM: return "DATA_REPL_NUM";
                case COL_DATA_VERSION: return "DATA_VERSION";
                case COL_DATA_TYPE_NAME: return "DATA_TYPE_NAME";
                case COL_DATA_SIZE: return "DATA_SIZE";
                case COL_D_RESC_NAME: return "DATA_RESC_NAME";
                case COL_D_DATA_PATH: return "DATA_PATH";
                case COL_D_OWNER_NAME: return "DATA_OWNER_NAME";
                case COL_D_OWNER_ZONE: return "DATA_OWNER_ZONE";
                case COL_D_REPL_STATUS: return "DATA_REPL_STATUS";
                case COL_D_DATA_STATUS: return "DATA_DATA_STATUS";
                case COL_D_DATA_CHECKSUM: return "DATA_CHECKSUM";
                case COL_D_EXPIRY: return "DATA_EXPIRY";
                case COL_D_MAP_ID: return "DATA_MAP_ID";
                case COL_D_COMMENTS: return "DATA_COMMENTS";
                case COL_D_CREATE_TIME: return "DATA_CREATE_TIME";
                case COL_D_MODIFY_TIME: return "DATA_MODIFY_TIME";
                case COL_DATA_MODE: return "DATA_MODE";
                case COL_D_RESC_HIER: return "DATA_RESC_HIER";
                case COL_D_RESC_ID: return "D_RESC_ID";
                
                case COL_COLL_ID: return "COLL_ID";
                case COL_COLL_NAME: return "COLL_NAME";
                case COL_COLL_PARENT_NAME: return "COLL_PARENT_NAME";
                case COL_COLL_OWNER_NAME: return "COLL_OWNER_NAME";
                case COL_COLL_OWNER_ZONE: return "COLL_OWNER_ZONE";
                case COL_COLL_CREATE_TIME: return "COLL_CREATE_TIME";
                case COL_COLL_MODIFY_TIME: return "COLL_MODIFY_TIME";
                case COL_COLL_TYPE: return "COLL_TYPE";
                case COL_COLL_INFO1: return "COLL_INFO1";
                case COL_COLL_INFO2: return "COLL_INFO2";
                
                case COL_USER_ID: return "USER_ID";
                case COL_USER_NAME: return "USER_NAME";
                case COL_USER_ZONE: return "USER_ZONE";
                
                case COL_ZONE_ID: return "ZONE_ID";
                case COL_ZONE_NAME: return "ZONE_NAME";
                
                case COL_R_RESC_ID: return "RESC_ID";
                case COL_R_RESC_NAME: return "RESC_NAME";
                case COL_R_ZONE_NAME: return "ZONE_NAME";
                case COL_R_TYPE_NAME: return "RESC_TYPE_NAME";
                case COL_R_LOC: return "RESC_LOC";
                case COL_R_VAULT_PATH: return "RESC_VAULT_PATH";
                case COL_R_FREE_SPACE: return "RESC_FREE_SPACE";
                case COL_R_RESC_INFO: return "RESC_INFO";
                case COL_R_RESC_COMMENT: return "RESC_COMMENT";
                case COL_R_RESC_STATUS: return "RESC_STATUS";
                case COL_R_RESC_CHILDREN: return "RESC_CHILDREN";
                case COL_R_RESC_CONTEXT: return "RESC_CONTEXT";
                case COL_R_RESC_PARENT: return "RESC_PARENT";
                case COL_R_RESC_PARENT_CONTEXT: return "RESC_PARENT_CONTEXT";

                case COL_META_DATA_ATTR_NAME: return "META_DATA_ATTR_NAME";
                case COL_META_DATA_ATTR_VALUE: return "META_DATA_ATTR_VALUE";
                case COL_META_DATA_ATTR_UNITS: return "META_DATA_ATTR_UNITS";
                case COL_META_DATA_ATTR_ID: return "META_DATA_ATTR_ID";
                case COL_META_COLL_ATTR_NAME: return "META_COLL_ATTR_NAME";
                case COL_META_COLL_ATTR_VALUE: return "META_COLL_ATTR_VALUE";
                case COL_META_COLL_ATTR_UNITS: return "META_COLL_ATTR_UNITS";
                case COL_META_COLL_ATTR_ID: return "META_COLL_ATTR_ID";
                case COL_META_RESC_ATTR_NAME: return "META_RESC_ATTR_NAME";
                case COL_META_RESC_ATTR_VALUE: return "META_RESC_ATTR_VALUE";
                case COL_META_RESC_ATTR_UNITS: return "META_RESC_ATTR_UNITS";
                case COL_META_RESC_ATTR_ID: return "META_RESC_ATTR_ID";
                case COL_META_USER_ATTR_NAME: return "META_USER_ATTR_NAME";
                case COL_META_USER_ATTR_VALUE: return "META_USER_ATTR_VALUE";
                case COL_META_USER_ATTR_UNITS: return "META_USER_ATTR_UNITS";
                case COL_META_USER_ATTR_ID: return "META_USER_ATTR_ID";

                case COL_USER_GROUP_ID: return "USER_GROUP_ID";
                case COL_USER_GROUP_NAME: return "USER_GROUP_NAME";

                case COL_DATA_ACCESS_NAME: return "DATA_ACCESS_NAME";
                case COL_DATA_ACCESS_TYPE: return "DATA_ACCESS_TYPE";
                case COL_DATA_TOKEN_NAMESPACE: return "DATA_TOKEN_NAMESPACE";
                case COL_TICKET_ID: return "TICKET_ID";
                case COL_TICKET_STRING: return "TICKET_STRING";
                case COL_TICKET_TYPE: return "TICKET_TYPE";
                case COL_TICKET_USES_LIMIT: return "TICKET_USES_LIMIT";
                case COL_TICKET_USES_COUNT: return "TICKET_USES_COUNT";
                case COL_TICKET_EXPIRY_TS: return "TICKET_EXPIRY_TS";
                case COL_TICKET_CREATE_TIME: return "TICKET_CREATE_TIME";
                case COL_TICKET_MODIFY_TIME: return "TICKET_MODIFY_TIME";
                case COL_TICKET_WRITE_FILE_COUNT: return "TICKET_WRITE_FILE_COUNT";
                case COL_TICKET_WRITE_FILE_LIMIT: return "TICKET_WRITE_FILE_LIMIT";
                case COL_TICKET_WRITE_BYTE_COUNT: return "TICKET_WRITE_BYTE_COUNT";
                case COL_TICKET_WRITE_BYTE_LIMIT: return "TICKET_WRITE_BYTE_LIMIT";
                case COL_TICKET_DATA_NAME: return "TICKET_DATA_NAME";
                case COL_TICKET_COLL_NAME: return "TICKET_COLL_NAME";
                case COL_TICKET_OWNER_NAME: return "TICKET_OWNER_NAME";

                case COL_AUDIT_OBJ_ID: return "AUDIT_OBJ_ID";
                case COL_AUDIT_USER_ID: return "AUDIT_USER_ID";
                case COL_AUDIT_ACTION_ID: return "AUDIT_ACTION_ID";
                case COL_AUDIT_COMMENT: return "AUDIT_COMMENT";
                case COL_AUDIT_CREATE_TIME: return "AUDIT_CREATE_TIME";
                case COL_AUDIT_MODIFY_TIME: return "AUDIT_MODIFY_TIME";

                case COL_SL_HOST_NAME: return "SL_HOST_NAME";
                case COL_SL_RESC_NAME: return "SL_RESC_NAME";
                case COL_SL_CPU_USED: return "SL_CPU_USED";
                case COL_SL_MEM_USED: return "SL_MEM_USED";
                case COL_SL_SWAP_USED: return "SL_SWAP_USED";
                case COL_SL_RUNQ_LOAD: return "SL_RUNQ_LOAD";
                case COL_SL_DISK_SPACE: return "SL_DISK_SPACE";
                case COL_SL_NET_INPUT: return "SL_NET_INPUT";
                case COL_SL_NET_OUTPUT: return "SL_NET_OUTPUT";
                case COL_SL_CREATE_TIME: return "SL_CREATE_TIME";

                case COL_RULE_ID: return "RULE_ID";
                case COL_RULE_NAME: return "RULE_NAME";
                case COL_RULE_BODY: return "RULE_BODY";
                case COL_RULE_OWNER_NAME: return "RULE_OWNER_NAME";
                case COL_RULE_CREATE_TIME: return "RULE_CREATE_TIME";
                case COL_RULE_MODIFY_TIME: return "RULE_MODIFY_TIME";

                case COL_DVM_ID: return "DVM_ID";
                case COL_DVM_BASE_NAME: return "DVM_BASE_NAME";
                case COL_DVM_EXT_VAR_NAME: return "DVM_EXT_VAR_NAME";
                case COL_DVM_INT_MAP_PATH: return "DVM_INT_MAP_PATH";

                case COL_FNM_ID: return "FNM_ID";
                case COL_FNM_BASE_NAME: return "FNM_BASE_NAME";
                case COL_FNM_EXT_FUNC_NAME: return "FNM_EXT_FUNC_NAME";
                case COL_FNM_INT_FUNC_NAME: return "FNM_INT_FUNC_NAME";

                case COL_QUOTA_USER_ID: return "QUOTA_USER_ID";
                case COL_QUOTA_RESC_ID: return "QUOTA_RESC_ID";
                case COL_QUOTA_LIMIT: return "QUOTA_LIMIT";
                case COL_QUOTA_OVER: return "QUOTA_OVER";
                
                default: 
                   return "";
            }
        };

        // Determine likely root alias for type-safe resolution
        std::string likely_root = "DataObject";
        for (int i = 0; i < _inp->selectInp.len; ++i) {
            int inx = _inp->selectInp.inx[i] & ~ORDER_BY & ~ORDER_BY_DESC;
            if (inx >= 500 && inx < 600) { likely_root = "Collection"; break; }
            if (inx >= 600 && inx < 700) { likely_root = "Resource"; break; }
            if (inx >= 200 && inx < 300) { likely_root = "User"; break; }
            if (inx >= 100 && inx < 200) { likely_root = "Zone"; break; }
        }

        // 1. Projections
        for (int i = 0; i < _inp->selectInp.len; ++i) {
            int inx = _inp->selectInp.inx[i];
            int pure_inx = inx & ~ORDER_BY & ~ORDER_BY_DESC;
            std::string name = get_col_name(pure_inx);
            
            if (name.empty()) {
                gq2::column dummy("DATA_ID"); 
                ast.projections.push_back(dummy);
            } else {
                gq2::column col(name);
                ast.projections.push_back(col);
                if (inx & ORDER_BY) {
                    gq2::sort_expression sort; sort.expr = col; sort.ascending_order = true;
                    ast.order_by.sort_expressions.push_back(sort);
                } else if (inx & ORDER_BY_DESC) {
                    gq2::sort_expression sort; sort.expr = col; sort.ascending_order = false;
                    ast.order_by.sort_expressions.push_back(sort);
                }
            }
        }

        // 2. Conditions & Path Resolution
        bool resolved_start = false;
        for (int i = 0; i < _inp->sqlCondInp.len; ++i) {
            int inx = _inp->sqlCondInp.inx[i];
            std::string cond(_inp->sqlCondInp.value[i]);
            std::string name = get_col_name(inx);
            
            rodsLog(LOG_NOTICE, "L3_BRIDGE: Raw Condition Received: index=%d (%s), value='%s'", inx, name.c_str(), cond.c_str());

            if (!name.empty()) {
                gq2::column col(name);
                std::regex eq_regex("^\\s*=\\s*'(.*)'\\s*$");
                std::regex like_regex("^\\s*like\\s*'(.*)'\\s*$");
                std::regex eq_or_like_regex("^\\s*=\\s*'(.*)'\\s*\\|\\|\\s*like\\s*'(.*)'\\s*$");
                std::regex parent_regex("^\\s*parent_of\\s*'(.*)'\\s*$");

                std::smatch match;
                if (std::regex_match(cond, match, eq_regex)) {
                    std::string literal = match[1].str();
                    if ((inx == COL_COLL_NAME || inx == COL_DATA_NAME || inx == COL_USER_NAME || inx == COL_ZONE_NAME) && !resolved_start) {
                        snowflake_id_t sid = 0; EntityType type;
                        if (_catalog->resolve_path(literal, sid, type).ok()) {
                            _starting_nodes.push_back(sid);
                            resolved_start = true;
                            rodsLog(LOG_NOTICE, "L3_BRIDGE: Resolved path [%s] to starting node %lx", literal.c_str(), sid);
                        }
                    }
                    ast.conditions.push_back(gq2::condition(col, gq2::condition_equal(literal)));
                } else if (std::regex_match(cond, match, eq_or_like_regex)) {
                    ast.conditions.push_back(gq2::condition(col, gq2::condition_like(match[2].str())));
                } else if (std::regex_match(cond, match, like_regex)) {
                    ast.conditions.push_back(gq2::condition(col, gq2::condition_like(match[1].str())));
                } else if (std::regex_match(cond, match, parent_regex)) {
                    irods::experimental::filesystem::path p(match[1].str());
                    std::string parent_path = p.parent_path().string();
                    if ((inx == COL_COLL_NAME) && !resolved_start) {
                        snowflake_id_t sid = 0; EntityType type;
                        if (_catalog->resolve_path(parent_path, sid, type).ok()) {
                            _starting_nodes.push_back(sid);
                            resolved_start = true;
                            rodsLog(LOG_NOTICE, "L3_BRIDGE: Resolved parent path [%s] to starting node %lx", parent_path.c_str(), sid);
                        }
                    }
                    ast.conditions.push_back(gq2::condition(col, gq2::condition_equal(parent_path)));
                }
            }
        }

        return ast;
    }

    /**
     * Packs L3KVG ResultSet back into legacy genQueryOut_t.
     */
    void pack_gq1_results(const ResultSet& _results, genQueryInp_t* _inp, genQueryOut_t* _out) {
        rodsLog(LOG_NOTICE, "L3_BRIDGE: Packing %zu rows into GQ1 output", _results.row_count());
        _out->rowCnt = _results.row_count();
        _out->attriCnt = _inp->selectInp.len;
        _out->continueInx = 0; 
        _out->totalRowCount = _out->rowCnt;

        for (int i = 0; i < _out->attriCnt; ++i) {
            int inx = _inp->selectInp.inx[i] & ~ORDER_BY & ~ORDER_BY_DESC;
            _out->sqlResult[i].attriInx = inx;
            
            int col_len = 2700; 
            _out->sqlResult[i].len = col_len;
            _out->sqlResult[i].value = (char*)malloc(_out->rowCnt * col_len);
            if (!_out->sqlResult[i].value) throw std::runtime_error("Failed to allocate result buffer");
            memset(_out->sqlResult[i].value, 0, _out->rowCnt * col_len);

            for (int r = 0; r < _out->rowCnt; ++r) {
                std::string val{_results.get_field(r, (size_t)i)};

                // Detect if the column is likely numeric to avoid std::stoll crashes
                bool is_string_col = (inx == COL_DATA_NAME || inx == COL_COLL_NAME || 
                                     inx == COL_USER_NAME || inx == COL_R_RESC_NAME || 
                                     inx == COL_D_DATA_PATH || inx == COL_D_RESC_HIER ||
                                     inx == COL_D_DATA_CHECKSUM || inx == COL_D_OWNER_NAME || 
                                     inx == COL_D_OWNER_ZONE || inx == COL_COLL_OWNER_NAME || 
                                     inx == COL_COLL_OWNER_ZONE || inx == COL_D_RESC_NAME ||
                                     inx == COL_ZONE_NAME || inx == COL_DATA_ACCESS_NAME ||
                                     inx == COL_DATA_TOKEN_NAMESPACE || inx == COL_COLL_ACCESS_NAME ||
                                     inx == COL_COLL_TOKEN_NAMESPACE || inx == COL_COLL_PARENT_NAME ||
                                     inx == COL_D_COMMENTS || inx == COL_DATA_TYPE_NAME ||
                                     inx == COL_D_EXPIRY || inx == COL_D_DATA_STATUS ||
                                     inx == COL_DATA_VERSION || inx == COL_D_MAP_ID ||
                                     inx == COL_COLL_INFO1 || inx == COL_COLL_INFO2 ||
                                     inx == COL_USER_TYPE || inx == COL_ZONE_TYPE ||
                                     inx == COL_R_TYPE_NAME || inx == COL_R_CLASS_NAME ||
                                     inx == COL_R_LOC || inx == COL_R_VAULT_PATH ||
                                     inx == COL_COLL_TYPE || inx == COL_DATA_ACCESS_TYPE ||
                                     inx == COL_COLL_ACCESS_TYPE || inx == COL_TICKET_STRING ||
                                     inx == COL_TICKET_TYPE || inx == COL_TICKET_DATA_NAME ||
                                     inx == COL_TICKET_COLL_NAME || inx == COL_TICKET_OWNER_NAME ||
                                     inx == COL_META_DATA_ATTR_NAME || inx == COL_META_DATA_ATTR_VALUE ||
                                     inx == COL_META_DATA_ATTR_UNITS || inx == COL_META_COLL_ATTR_NAME ||
                                     inx == COL_META_COLL_ATTR_VALUE || inx == COL_META_COLL_ATTR_UNITS ||
                                     inx == COL_META_RESC_ATTR_NAME || inx == COL_META_RESC_ATTR_VALUE ||
                                     inx == COL_META_RESC_ATTR_UNITS || inx == COL_META_USER_ATTR_NAME ||
                                     inx == COL_META_USER_ATTR_VALUE || inx == COL_META_USER_ATTR_UNITS ||
                                     inx == COL_USER_GROUP_NAME || inx == COL_R_RESC_INFO ||
                                     inx == COL_R_RESC_COMMENT || inx == COL_R_RESC_CHILDREN ||
                                     inx == COL_R_RESC_CONTEXT || inx == COL_R_RESC_PARENT ||
                                     inx == COL_R_RESC_PARENT_CONTEXT || inx == COL_R_RESC_STATUS ||
                                     inx == COL_RULE_NAME || inx == COL_RULE_BODY ||
                                     inx == COL_RULE_OWNER_NAME || inx == COL_DVM_BASE_NAME ||
                                     inx == COL_DVM_EXT_VAR_NAME || inx == COL_DVM_INT_MAP_PATH ||
                                     inx == COL_FNM_BASE_NAME || inx == COL_FNM_EXT_FUNC_NAME ||
                                     inx == COL_FNM_INT_FUNC_NAME || inx == COL_AUDIT_COMMENT ||
                                     inx == COL_SL_HOST_NAME || inx == COL_SL_RESC_NAME);

                if (val.empty()) {
                    if (is_string_col) {
                        val = ""; 
                    } else {
                        val = "0"; // Conservative default for all non-string columns
                    }
                }

                rodsLog(LOG_NOTICE, "L3_BRIDGE: Packing Col %d Row %d: [%s]", inx, r, val.c_str());
                strncpy(&_out->sqlResult[i].value[r * col_len], val.c_str(), col_len - 1);
            }
        }
    }

} // namespace irods::catalog::bridge
