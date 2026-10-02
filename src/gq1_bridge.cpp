#include "irods/rodsGenQuery.h"
#include "irods/catalog/gq2_compiler.hpp"
#include "irods/catalog/catalog_facade.hpp"
#include "irods/rodsLog.h"
#include "irods/filesystem/path.hpp"
#include <vector>
#include <string>
#include <map>
#include <unordered_set>
#include <regex>
#include <cstring>
#include <iostream>
#include <sstream>
#include <boost/algorithm/string.hpp>

namespace irods::catalog::bridge {

    static std::string unescape_sql_literal(std::string str) {
        size_t pos = 0;
        while ((pos = str.find("''", pos)) != std::string::npos) {
            str.replace(pos, 2, "'");
            pos += 1;
        }
        pos = 0;
        while ((pos = str.find("\\'", pos)) != std::string::npos) {
            str.replace(pos, 2, "'");
            pos += 1;
        }
        return str;
    }

    std::string get_col_name(int pure_inx) {
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
                case COL_D_ACCESS_TIME: return "DATA_ACCESS_TIME";
                
                case COL_ZONE_ID: return "ZONE_ID";
                case COL_ZONE_NAME: return "ZONE_NAME";
                case COL_ZONE_TYPE: return "ZONE_TYPE";
                case COL_ZONE_CONNECTION: return "ZONE_CONNECTION";
                case COL_ZONE_COMMENT: return "ZONE_COMMENT";
                case COL_ZONE_CREATE_TIME: return "ZONE_CREATE_TIME";
                case COL_ZONE_MODIFY_TIME: return "ZONE_MODIFY_TIME";

                case COL_USER_ID: return "USER_ID";
                case COL_USER_NAME: return "USER_NAME";
                case COL_USER_ZONE: return "USER_ZONE";
                case COL_USER_TYPE: return "USER_TYPE";
                case COL_USER_INFO: return "USER_INFO";
                case COL_USER_COMMENT: return "USER_COMMENT";
                case COL_USER_CREATE_TIME: return "USER_CREATE_TIME";
                case COL_USER_MODIFY_TIME: return "USER_MODIFY_TIME";
                case COL_USER_DN: return "USER_DN";
                case COL_USER_GROUP_ID: return "USER_GROUP_ID";
                case COL_USER_GROUP_NAME: return "USER_GROUP_NAME";

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
                case COL_COLL_INHERITANCE: return "COLL_INHERITANCE";
                case COL_COLL_COMMENTS: return "COLL_COMMENTS";

                case COL_DATA_ACCESS_TYPE: return "DATA_ACCESS_TYPE";
                case COL_DATA_ACCESS_NAME: return "DATA_ACCESS_NAME";
                case COL_DATA_TOKEN_NAMESPACE: return "DATA_TOKEN_NAMESPACE";
                case COL_DATA_ACCESS_USER_ID: return "DATA_ACCESS_USER_ID";
                case COL_DATA_ACCESS_DATA_ID: return "DATA_ACCESS_DATA_ID";

                case COL_COLL_ACCESS_TYPE: return "COLL_ACCESS_TYPE";
                case COL_COLL_ACCESS_NAME: return "COLL_ACCESS_NAME";
                case COL_COLL_TOKEN_NAMESPACE: return "COLL_TOKEN_NAMESPACE";
                case COL_COLL_ACCESS_USER_ID: return "COLL_ACCESS_USER_ID";
                case COL_COLL_ACCESS_COLL_ID: return "COLL_COLL_ACCESS_COLL_ID";

                case COL_R_RESC_ID: return "RESC_ID";
                case COL_R_RESC_NAME: return "RESC_NAME";
                case COL_R_ZONE_NAME: return "RESC_ZONE_NAME";
                case COL_R_TYPE_NAME: return "RESC_TYPE_NAME";
                case COL_R_CLASS_NAME: return "RESC_CLASS_NAME";
                case COL_R_LOC: return "RESC_LOC";
                case COL_R_VAULT_PATH: return "RESC_VAULT_PATH";
                case COL_R_FREE_SPACE: return "RESC_FREE_SPACE";
                case COL_R_FREE_SPACE_TIME: return "RESC_FREE_SPACE_TIME";
                case COL_R_RESC_INFO: return "RESC_INFO";
                case COL_R_RESC_COMMENT: return "RESC_COMMENT";
                case COL_R_RESC_STATUS: return "RESC_STATUS";
                case COL_R_RESC_CHILDREN: return "RESC_CHILDREN";
                case COL_R_RESC_CONTEXT: return "RESC_CONTEXT";
                case COL_R_RESC_PARENT: return "RESC_PARENT";
                case COL_R_RESC_PARENT_CONTEXT: return "RESC_PARENT_CONTEXT";
                case COL_R_CREATE_TIME: return "RESC_CREATE_TIME";
                case COL_R_MODIFY_TIME: return "RESC_MODIFY_TIME";

                case COL_MSRVC_ID: return "MSRVC_ID";
                case COL_MSRVC_NAME: return "MSRVC_NAME";
                case COL_MSRVC_SIGNATURE: return "MSRVC_SIGNATURE";
                case COL_MSRVC_DOXYGEN: return "MSRVC_DOXYGEN";
                case COL_MSRVC_VARIATIONS: return "MSRVC_VARIATIONS";
                case COL_MSRVC_STATUS: return "MSRVC_STATUS";
                case COL_MSRVC_OWNER_NAME: return "MSRVC_OWNER_NAME";
                case COL_MSRVC_OWNER_ZONE: return "MSRVC_OWNER_ZONE";
                case COL_MSRVC_COMMENT: return "MSRVC_COMMENT";
                case COL_MSRVC_CREATE_TIME: return "MSRVC_CREATE_TIME";
                case COL_MSRVC_MODIFY_TIME: return "MSRVC_MODIFY_TIME";
                case COL_MSRVC_VERSION: return "MSRVC_VERSION";
                case COL_MSRVC_HOST: return "MSRVC_HOST";
                case COL_MSRVC_LOCATION: return "MSRVC_LOCATION";
                case COL_MSRVC_LANGUAGE: return "MSRVC_LANGUAGE";
                case COL_MSRVC_TYPE_NAME: return "MSRVC_TYPE_NAME";
                case COL_MSRVC_MODULE_NAME: return "MSRVC_MODULE_NAME";

                case COL_MSRVC_VER_OWNER_NAME: return "MSRVC_VER_OWNER_NAME";
                case COL_MSRVC_VER_OWNER_ZONE: return "MSRVC_VER_OWNER_ZONE";
                case COL_MSRVC_VER_COMMENT: return "MSRVC_VER_COMMENT";
                case COL_MSRVC_VER_CREATE_TIME: return "MSRVC_VER_CREATE_TIME";
                case COL_MSRVC_VER_MODIFY_TIME: return "MSRVC_VER_MODIFY_TIME";

                case COL_MSRVC_ACCESS_TYPE: return "MSRVC_ACCESS_TYPE";
                case COL_MSRVC_ACCESS_NAME: return "MSRVC_ACCESS_NAME";
                case COL_MSRVC_TOKEN_NAMESPACE: return "MSRVC_TOKEN_NAMESPACE";

                case COL_QUOTA_USER_ID: return "QUOTA_USER_ID";
                case COL_QUOTA_USER_NAME: return "QUOTA_USER_NAME";
                case COL_QUOTA_USER_ZONE: return "QUOTA_USER_ZONE";
                case COL_QUOTA_RESC_ID: return "QUOTA_RESC_ID";
                case COL_QUOTA_RESC_NAME: return "QUOTA_RESC_NAME";
                case COL_QUOTA_LIMIT: return "QUOTA_LIMIT";
                case COL_QUOTA_OVER: return "QUOTA_OVER";
                case COL_QUOTA_USAGE: return "QUOTA_USAGE";

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
                case COL_TICKET_ALLOWED_HOST_TICKET_ID: return "TICKET_ALLOWED_HOST_TICKET_ID";
                case COL_TICKET_ALLOWED_HOST: return "TICKET_ALLOWED_HOST";
                case COL_TICKET_ALLOWED_USER_TICKET_ID: return "TICKET_ALLOWED_USER_TICKET_ID";
                case COL_TICKET_ALLOWED_USER_NAME: return "TICKET_ALLOWED_USER_NAME";
                case COL_TICKET_ALLOWED_GROUP_TICKET_ID: return "TICKET_ALLOWED_GROUP_TICKET_ID";
                case COL_TICKET_ALLOWED_GROUP_NAME: return "TICKET_ALLOWED_GROUP_NAME";
                case COL_TICKET_USER_ID: return "TICKET_USER_ID";
                case COL_TICKET_OBJECT_ID: return "TICKET_OBJECT_ID";
                case COL_TICKET_OBJECT_TYPE: return "TICKET_OBJECT_TYPE";
                case COL_TICKET_DATA_COLL_NAME: return "TICKET_DATA_COLL_NAME";
                case COL_TICKET_OWNER_ZONE: return "TICKET_OWNER_ZONE";

                case COL_COLL_USER_NAME: return "COLL_USER_NAME";
                case COL_COLL_USER_ZONE: return "COLL_USER_ZONE";
                case COL_DATA_USER_NAME: return "DATA_USER_NAME";
                case COL_DATA_USER_ZONE: return "DATA_USER_ZONE";

                case COL_RULE_EXEC_ID: return "RULE_EXEC_ID";
                case COL_RULE_EXEC_NAME: return "RULE_EXEC_NAME";
                case COL_RULE_EXEC_REI_FILE_PATH: return "RULE_EXEC_REI_FILE_PATH";
                case COL_RULE_EXEC_USER_NAME: return "RULE_EXEC_USER_NAME";
                case COL_RULE_EXEC_ADDRESS: return "RULE_EXEC_ADDRESS";
                case COL_RULE_EXEC_TIME: return "RULE_EXEC_TIME";
                case COL_RULE_EXEC_FREQUENCY: return "RULE_EXEC_FREQUENCY";
                case COL_RULE_EXEC_PRIORITY: return "RULE_EXEC_PRIORITY";
                case COL_RULE_EXEC_ESTIMATED_EXE_TIME: return "RULE_EXEC_ESTIMATED_EXE_TIME";
                case COL_RULE_EXEC_NOTIFICATION_ADDR: return "RULE_EXEC_NOTIFICATION_ADDR";
                case COL_RULE_EXEC_LAST_EXE_TIME: return "RULE_EXEC_LAST_EXE_TIME";
                case COL_RULE_EXEC_STATUS: return "RULE_EXEC_STATUS";
                case COL_RULE_EXEC_CONTEXT: return "RULE_EXEC_CONTEXT";
                case COL_RULE_EXEC_LOCK_HOST: return "RULE_EXEC_LOCK_HOST";
                case COL_RULE_EXEC_LOCK_HOST_PID: return "RULE_EXEC_LOCK_HOST_PID";
                case COL_RULE_EXEC_LOCK_TIME: return "RULE_EXEC_LOCK_TIME";

                case COL_META_DATA_ATTR_NAME: return "META_DATA_ATTR_NAME";
                case COL_META_DATA_ATTR_VALUE: return "META_DATA_ATTR_VALUE";
                case COL_META_DATA_ATTR_UNITS: return "META_DATA_ATTR_UNITS";
                case COL_META_DATA_ATTR_ID: return "META_DATA_ATTR_ID";
                case COL_META_DATA_CREATE_TIME: return "META_DATA_CREATE_TIME";
                case COL_META_DATA_MODIFY_TIME: return "META_DATA_MODIFY_TIME";

                case COL_META_COLL_ATTR_NAME: return "META_COLL_ATTR_NAME";
                case COL_META_COLL_ATTR_VALUE: return "META_COLL_ATTR_VALUE";
                case COL_META_COLL_ATTR_UNITS: return "META_COLL_ATTR_UNITS";
                case COL_META_COLL_ATTR_ID: return "META_COLL_ATTR_ID";
                case COL_META_COLL_CREATE_TIME: return "META_COLL_CREATE_TIME";
                case COL_META_COLL_MODIFY_TIME: return "META_COLL_MODIFY_TIME";

                case COL_META_RESC_ATTR_NAME: return "META_RESC_ATTR_NAME";
                case COL_META_RESC_ATTR_VALUE: return "META_RESC_ATTR_VALUE";
                case COL_META_RESC_ATTR_UNITS: return "META_RESC_ATTR_UNITS";
                case COL_META_RESC_ATTR_ID: return "META_RESC_ATTR_ID";
                case COL_META_RESC_CREATE_TIME: return "META_RESC_CREATE_TIME";
                case COL_META_RESC_MODIFY_TIME: return "META_RESC_MODIFY_TIME";

                case COL_META_USER_ATTR_NAME: return "META_USER_ATTR_NAME";
                case COL_META_USER_ATTR_VALUE: return "META_USER_ATTR_VALUE";
                case COL_META_USER_ATTR_UNITS: return "META_USER_ATTR_UNITS";
                case COL_META_USER_ATTR_ID: return "META_USER_ATTR_ID";
                case COL_META_USER_CREATE_TIME: return "META_USER_CREATE_TIME";
                case COL_META_USER_MODIFY_TIME: return "META_USER_MODIFY_TIME";

                default: return "";
            }
    }

    int get_pure_inx(int inx) {
        int pure_inx = inx;
        if (pure_inx >= 0x400 && get_col_name(pure_inx).empty()) {
            int stripped = pure_inx & ~ORDER_BY & ~ORDER_BY_DESC;
            if (!get_col_name(stripped).empty()) {
                pure_inx = stripped;
            }
        }
        return pure_inx;
    }

    /**
     * Synthesizes a GenQuery2 AST from a legacy GenQuery1 input.
     */
    irods::experimental::genquery2::select synthesize_gq2_ast(genQueryInp_t* _inp, CatalogFacade* _catalog, std::vector<uint64_t>& _starting_nodes) {
        rodsLog(LOG_NOTICE, "L3_BRIDGE: Synthesizing GQ2 AST from GQ1 Input. Projections: %d, Conditions: %d", _inp->selectInp.len, _inp->sqlCondInp.len);
        
        namespace gq2 = irods::experimental::genquery2;
        gq2::select ast;
        ast.distinct = true;

        // Determine likely root alias for type-safe resolution
        auto get_entity_for_inx = [](int inx) -> std::pair<std::string, int> {
            if (inx >= 1000 && inx < 1100) return {"Rule", 10};
            if (inx >= 2200 && inx < 2230) return {"Ticket", 8};
            if (inx == COL_USER_GROUP_ID || inx == COL_USER_GROUP_NAME || (inx >= 900 && inx < 910)) return {"Group", 4};
            if (inx >= 400 && inx < 500)   return {"DataObject", 5};
            if (inx >= 700 && inx < 710)   return {"DataObject", 5};
            if (inx >= 1310 && inx < 1320) return {"DataObject", 5};
            if (inx >= 710 && inx < 720)   return {"Collection", 4};
            if (inx >= 500 && inx < 600)   return {"Collection", 4};
            if (inx >= 1300 && inx < 1310) return {"Collection", 4};
            if (inx >= 720 && inx < 730)   return {"Resource", 3};
            if (inx >= 300 && inx < 400)   return {"Resource", 3};
            if (inx >= 200 && inx < 300)   return {"User", 3};
            if (inx >= 100 && inx < 200)   return {"Zone", 2};
            if (inx >= 600 && inx < 700)   return {"Metadata", 1};
            return {"", 0};
        };

        std::string likely_root = "DataObject";
        int best_prio = 0;

        for (int i = 0; i < _inp->selectInp.len; ++i) {
            int pure_inx = get_pure_inx(_inp->selectInp.inx[i]);
            auto [entity, prio] = get_entity_for_inx(pure_inx);
            if (prio > best_prio) {
                best_prio = prio;
                likely_root = entity;
            }
        }
        for (int i = 0; i < _inp->sqlCondInp.len; ++i) {
            int pure_inx = get_pure_inx(_inp->sqlCondInp.inx[i]);
            auto [entity, prio] = get_entity_for_inx(pure_inx);
            if (prio > best_prio) {
                best_prio = prio;
                likely_root = entity;
            }
        }
        if (likely_root.empty()) {
            likely_root = "DataObject";
        }
        ast.from_entity = likely_root;

        // 1. Projections
        for (int i = 0; i < _inp->selectInp.len; ++i) {
            int inx = _inp->selectInp.inx[i];
            int pure_inx = get_pure_inx(inx);
            std::string name = get_col_name(pure_inx);
            rodsLog(LOG_NOTICE, "L3_BRIDGE: Select Column %d: inx=%d, pure_inx=%d, name='%s'", i, inx, pure_inx, name.c_str());
            
            if (name.empty()) {
                gq2::column dummy("DATA_ID"); 
                ast.projections.push_back(dummy);
            } else {
                gq2::column col(name);
                int sel_val = _inp->selectInp.value ? _inp->selectInp.value[i] : 0;
                int agg_fn = sel_val & ~(ORDER_BY | ORDER_BY_DESC);
                if (agg_fn == SELECT_COUNT) {
                    gq2::function fn;
                    fn.name = "COUNT";
                    fn.arguments.push_back(col);
                    ast.projections.push_back(fn);
                } else if (agg_fn == SELECT_MIN) {
                    gq2::function fn;
                    fn.name = "MIN";
                    fn.arguments.push_back(col);
                    ast.projections.push_back(fn);
                } else if (agg_fn == SELECT_MAX) {
                    gq2::function fn;
                    fn.name = "MAX";
                    fn.arguments.push_back(col);
                    ast.projections.push_back(fn);
                } else if (agg_fn == SELECT_SUM) {
                    gq2::function fn;
                    fn.name = "SUM";
                    fn.arguments.push_back(col);
                    ast.projections.push_back(fn);
                } else if (agg_fn == SELECT_AVG) {
                    gq2::function fn;
                    fn.name = "AVG";
                    fn.arguments.push_back(col);
                    ast.projections.push_back(fn);
                } else {
                    ast.projections.push_back(col);
                }

                bool has_order_by = _inp->selectInp.value && (_inp->selectInp.value[i] & ORDER_BY);
                bool has_order_by_desc = _inp->selectInp.value && (_inp->selectInp.value[i] & ORDER_BY_DESC);
                if (has_order_by) {
                    gq2::sort_expression sort; sort.expr = col; sort.ascending_order = true;
                    ast.order_by.sort_expressions.push_back(sort);
                } else if (has_order_by_desc) {
                    gq2::sort_expression sort; sort.expr = col; sort.ascending_order = false;
                    ast.order_by.sort_expressions.push_back(sort);
                }
            }
        }

        // Limit and Offset
        if (_inp->maxRows > 0) {
            ast.range.number_of_rows = std::to_string(_inp->maxRows);
        }
        if (_inp->continueInx > 0) {
            ast.range.offset = std::to_string(_inp->continueInx);
        }

        // 2. Conditions & Path Resolution
        bool resolved_start = false;
        int best_start_priority = -1;
        auto extract_literal = [](const std::smatch& m) -> std::string {
            if (m[1].matched) {
                return unescape_sql_literal(m[1].str());
            }
            if (m[2].matched) {
                return unescape_sql_literal(m[2].str());
            }
            if (m[3].matched) {
                std::string s = m[3].str();
                if (s.size() >= 2 && s.front() == '\'' && s.back() == '\'') {
                    return unescape_sql_literal(s.substr(1, s.size() - 2));
                }
                return s;
            }
            return "";
        };

        std::regex eq_regex(R"(^\s*=\s*(?:'((?:[^'\\]|\\.|'')*)'|'(.*)'|(\S+))\s*$)");
        std::regex ne_regex(R"(^\s*(?:!=|<>)\s*(?:'((?:[^'\\]|\\.|'')*)'|'(.*)'|(\S+))\s*$)");
        std::regex le_regex(R"(^\s*<=\s*(?:'((?:[^'\\]|\\.|'')*)'|'(.*)'|(\S+))\s*$)");
        std::regex ge_regex(R"(^\s*>=\s*(?:'((?:[^'\\]|\\.|'')*)'|'(.*)'|(\S+))\s*$)");
        std::regex lt_regex(R"(^\s*<\s*(?:'((?:[^'\\]|\\.|'')*)'|'(.*)'|(\S+))\s*$)");
        std::regex gt_regex(R"(^\s*>\s*(?:'((?:[^'\\]|\\.|'')*)'|'(.*)'|(\S+))\s*$)");
        std::regex like_regex(R"(^\s*like\s*(?:'((?:[^'\\]|\\.|'')*)'|'(.*)'|(\S+))\s*$)", std::regex_constants::icase);
        std::regex not_like_regex(R"(^\s*not\s+like\s*(?:'((?:[^'\\]|\\.|'')*)'|'(.*)'|(\S+))\s*$)", std::regex_constants::icase);
        std::regex eq_or_like_regex(R"(^\s*=\s*'(.*?)'\s*\|\|\s*like\s*'(.*)'\s*$)", std::regex_constants::icase);
        std::regex like_or_eq_regex(R"(^\s*like\s*'(.*?)'\s*\|\|\s*=\s*'(.*)'\s*$)", std::regex_constants::icase);
        std::regex parent_regex(R"(^\s*parent_of\s*'(.*)'\s*$)");
        std::regex in_clause_regex(R"(IN\s*\()", std::regex_constants::icase);
        std::regex quoted_literal_regex(R"('((?:[^'\\]|\\.|'')*)')");

        // Pass 0: Combined COLL_NAME and DATA_NAME resolution (highest priority)
        std::string target_coll_name;
        std::string target_data_name;
        for (int i = 0; i < _inp->sqlCondInp.len; ++i) {
            int inx = _inp->sqlCondInp.inx[i];
            std::string cond(_inp->sqlCondInp.value[i]);
            std::smatch match;
            if (cond.find("||") == std::string::npos && std::regex_match(cond, match, eq_regex)) {
                std::string literal = extract_literal(match);
                if (inx == COL_COLL_NAME) {
                    target_coll_name = literal;
                } else if (inx == COL_DATA_NAME) {
                    target_data_name = literal;
                }
            }
        }
        if (!target_coll_name.empty() && !target_data_name.empty() && _catalog != nullptr) {
            std::string full_path = (target_coll_name == "/" ? "/" + target_data_name : target_coll_name + "/" + target_data_name);
            snowflake_id_t sid = 0;
            EntityType type;
            if (_catalog->resolve_path(full_path, sid, type).ok() && type == EntityType::DataObject) {
                _starting_nodes.clear();
                _starting_nodes.push_back(sid);
                resolved_start = true;
                best_start_priority = 10;
            }
        }

        // Pass 1: Find best starting node
        for (int i = 0; i < _inp->sqlCondInp.len; ++i) {
            int inx = _inp->sqlCondInp.inx[i];
            std::string cond(_inp->sqlCondInp.value[i]);
            std::smatch match;

            if (cond.find("||") == std::string::npos && std::regex_match(cond, match, eq_regex)) {
                std::string literal = extract_literal(match);
                if (inx == COL_DATA_ACCESS_DATA_ID || inx == COL_D_DATA_ID) {
                    if (_catalog != nullptr && best_start_priority < 4) {
                        try {
                            uint64_t data_id = std::stoull(literal);
                            snowflake_id_t sid = _catalog->make_id(EntityType::DataObject, data_id);
                            if (sid) {
                                _starting_nodes.clear();
                                _starting_nodes.push_back(sid);
                                resolved_start = true;
                                best_start_priority = 4;
                            }
                        } catch (...) {}
                    }
                } else if (inx == COL_COLL_ACCESS_COLL_ID || inx == COL_COLL_ID) {
                    if (_catalog != nullptr && best_start_priority < 3) {
                        try {
                            uint64_t coll_id = std::stoull(literal);
                            snowflake_id_t sid = _catalog->make_id(EntityType::Collection, coll_id);
                            if (sid) {
                                if (likely_root == "DataObject") {
                                    auto child_nodes = _catalog->get_client()->get_neighbors_async(_catalog->get_cluster_id(), sid, "CONTAINS", 0.0).get();
                                    _starting_nodes = std::move(child_nodes);
                                    if (_starting_nodes.empty()) {
                                        _starting_nodes.push_back(0xFFFFFFFFFFFFFFFFULL);
                                    }
                                } else {
                                    _starting_nodes.clear();
                                    _starting_nodes.push_back(sid);
                                }
                                resolved_start = true;
                                best_start_priority = 3;
                            }
                        } catch (...) {}
                    }
                } else if (inx == COL_DATA_NAME) {
                    if (_catalog != nullptr && best_start_priority < 4 && literal.starts_with('/')) {
                        snowflake_id_t sid = 0; EntityType type;
                        if (_catalog->resolve_path(literal, sid, type).ok()) {
                            _starting_nodes.clear();
                            _starting_nodes.push_back(sid);
                            resolved_start = true;
                            best_start_priority = 4;
                        }
                    }
                } else if (inx == COL_COLL_NAME) {
                    if (_catalog != nullptr && best_start_priority < 3) {
                        snowflake_id_t sid = 0; EntityType type;
                        if (_catalog->resolve_path(literal, sid, type).ok()) {
                            if (likely_root == "DataObject") {
                                auto child_nodes = _catalog->get_client()->get_neighbors_async(_catalog->get_cluster_id(), sid, "CONTAINS", 0.0).get();
                                _starting_nodes = std::move(child_nodes);
                                if (_starting_nodes.empty()) {
                                    _starting_nodes.push_back(0xFFFFFFFFFFFFFFFFULL);
                                }
                            } else {
                                _starting_nodes.clear();
                                _starting_nodes.push_back(sid);
                            }
                            resolved_start = true;
                            best_start_priority = 3;
                        } else if (likely_root == "DataObject") {
                            _starting_nodes.clear();
                            _starting_nodes.push_back(0xFFFFFFFFFFFFFFFFULL);
                            resolved_start = true;
                            best_start_priority = 3;
                        }
                    }
                } else if (inx == COL_COLL_PARENT_NAME) {
                    if (_catalog != nullptr && best_start_priority < 2) {
                        snowflake_id_t parent_sid = 0; EntityType type;
                        if (_catalog->resolve_path(literal, parent_sid, type).ok()) {
                            auto child_nodes = _catalog->get_client()->get_neighbors_async(_catalog->get_cluster_id(), parent_sid, "CONTAINS", 0.0).get();
                            _starting_nodes = std::move(child_nodes);
                            resolved_start = true;
                            best_start_priority = 2;
                        }
                    }
                } else if (inx == COL_USER_NAME || inx == COL_USER_GROUP_NAME) {
                    if (_catalog != nullptr && (likely_root == "User" || likely_root == "Group" || best_start_priority < 1)) {
                        snowflake_id_t sid = _catalog->resolve_id_from_index(EntityType::User, "n", literal);
                        if (sid) {
                            _starting_nodes.clear();
                            _starting_nodes.push_back(sid);
                            resolved_start = true;
                            best_start_priority = (likely_root == "User" || likely_root == "Group" ? 4 : 1);
                        }
                    }
                } else if (inx == COL_USER_ID || inx == COL_USER_GROUP_ID) {
                    if (_catalog != nullptr && (likely_root == "User" || likely_root == "Group" || best_start_priority < 1)) {
                        try {
                            snowflake_id_t sid = _catalog->make_id(EntityType::User, std::stoull(literal));
                            if (sid) {
                                _starting_nodes.clear();
                                _starting_nodes.push_back(sid);
                                resolved_start = true;
                                best_start_priority = (likely_root == "User" || likely_root == "Group" ? 4 : 1);
                            }
                        } catch (...) {}
                    }
                } else if (inx == COL_R_RESC_NAME) {
                    if (_catalog != nullptr && (likely_root == "Resource" || best_start_priority < 1)) {
                        snowflake_id_t sid = _catalog->resolve_id_from_index(EntityType::Resource, "n", literal);
                        if (sid) {
                            _starting_nodes.clear();
                            _starting_nodes.push_back(sid);
                            resolved_start = true;
                            best_start_priority = (likely_root == "Resource" ? 4 : 1);
                        }
                    }
                } else if (inx == COL_R_RESC_ID) {
                    if (_catalog != nullptr && (likely_root == "Resource" || best_start_priority < 1)) {
                        try {
                            snowflake_id_t sid = _catalog->make_id(EntityType::Resource, std::stoull(literal));
                            if (sid) {
                                _starting_nodes.clear();
                                _starting_nodes.push_back(sid);
                                resolved_start = true;
                                best_start_priority = (likely_root == "Resource" ? 4 : 1);
                            }
                        } catch (...) {}
                    }
                } else if (inx == COL_RULE_EXEC_ID) {
                    if (_catalog != nullptr && best_start_priority < 4) {
                        try {
                            uint64_t rid_num = std::stoull(literal);
                            snowflake_id_t sid = _catalog->make_id(EntityType::Rule, rid_num);
                            _starting_nodes.clear();
                            _starting_nodes.push_back(sid);
                            resolved_start = true;
                            best_start_priority = 4;
                        } catch (...) {}
                    }
                } else if (inx == COL_TICKET_STRING) {
                    if (_catalog != nullptr && (likely_root == "Ticket" || best_start_priority < 4)) {
                        snowflake_id_t sid = _catalog->resolve_id_from_index(EntityType::Ticket, "s", literal);
                        if (sid) {
                            _starting_nodes.clear();
                            _starting_nodes.push_back(sid);
                            resolved_start = true;
                            best_start_priority = 4;
                        }
                    }
                } else if (inx == COL_TICKET_ID ||
                           inx == COL_TICKET_ALLOWED_HOST_TICKET_ID ||
                           inx == COL_TICKET_ALLOWED_USER_TICKET_ID ||
                           inx == COL_TICKET_ALLOWED_GROUP_TICKET_ID) {
                    if (_catalog != nullptr && (likely_root == "Ticket" || best_start_priority < 4)) {
                        try {
                            snowflake_id_t sid = _catalog->make_id(EntityType::Ticket, std::stoull(literal));
                            if (sid) {
                                _starting_nodes.clear();
                                _starting_nodes.push_back(sid);
                                resolved_start = true;
                                best_start_priority = 4;
                            }
                        } catch (...) {}
                    }
                } else if (inx == COL_TICKET_DATA_NAME) {
                    if (_catalog != nullptr && best_start_priority < 4) {
                        snowflake_id_t did = _catalog->resolve_id_from_index(EntityType::DataObject, "n", literal);
                        if (!did) {
                            EntityType dtype;
                            _catalog->resolve_path(literal, did, dtype);
                        }
                        if (did) {
                            auto t_nodes = _catalog->get_client()->get_in_neighbors_async(_catalog->get_cluster_id(), did, "FOR_OBJECT").get();
                            if (!t_nodes.empty()) {
                                _starting_nodes = std::move(t_nodes);
                                resolved_start = true;
                                best_start_priority = 4;
                                rodsLog(LOG_NOTICE, "L3_BRIDGE: Resolved %zu Ticket starting nodes from DataObject '%s' via FOR_OBJECT", _starting_nodes.size(), literal.c_str());
                            }
                        }
                    }
                } else if (inx == COL_TICKET_COLL_NAME) {
                    if (_catalog != nullptr && best_start_priority < 4) {
                        snowflake_id_t cid = _catalog->resolve_id_from_index(EntityType::Collection, "n", literal);
                        if (!cid) {
                            EntityType ctype;
                            _catalog->resolve_path(literal, cid, ctype);
                        }
                        if (cid) {
                            auto t_nodes = _catalog->get_client()->get_in_neighbors_async(_catalog->get_cluster_id(), cid, "FOR_OBJECT").get();
                            if (!t_nodes.empty()) {
                                _starting_nodes = std::move(t_nodes);
                                resolved_start = true;
                                best_start_priority = 4;
                                rodsLog(LOG_NOTICE, "L3_BRIDGE: Resolved %zu Ticket starting nodes from Collection '%s' via FOR_OBJECT", _starting_nodes.size(), literal.c_str());
                            }
                        }
                    }
                } else if (inx == COL_ZONE_NAME || inx == COL_ZONE_ID) {
                    if (_catalog != nullptr && best_start_priority < 1) {
                        snowflake_id_t zid = 0;
                        if (inx == COL_ZONE_NAME) {
                            zid = _catalog->resolve_id_from_index(EntityType::Zone, "n", literal);
                        } else {
                            zid = _catalog->resolve_id_from_index(EntityType::Zone, "id", literal);
                        }
                        if (!zid) {
                            zid = _catalog->get_zone_id(literal);
                        }
                        _starting_nodes.clear();
                        _starting_nodes.push_back(zid);
                        resolved_start = true;
                        best_start_priority = 1;
                    }
                }
            } else if (std::regex_match(cond, match, eq_or_like_regex) || std::regex_match(cond, match, like_or_eq_regex)) {
                std::string target_coll;
                if (std::regex_match(cond, match, eq_or_like_regex)) {
                    target_coll = unescape_sql_literal(match[1].str());
                } else {
                    target_coll = unescape_sql_literal(match[2].str());
                }
                if (inx == COL_COLL_NAME && _catalog != nullptr && best_start_priority < 3) {
                    snowflake_id_t sid = 0; EntityType type;
                    if (_catalog->resolve_path(target_coll, sid, type).ok() && type == EntityType::Collection) {
                        std::vector<snowflake_id_t> coll_ids;
                        _catalog->get_collection_subtree_ids(sid, coll_ids);
                        if (!coll_ids.empty()) {
                            if (likely_root == "DataObject") {
                                std::vector<snowflake_id_t> data_ids;
                                for (auto cid : coll_ids) {
                                    auto children = _catalog->get_client()->get_neighbors_async(_catalog->get_cluster_id(), cid, "CONTAINS", 0.0).get();
                                    data_ids.insert(data_ids.end(), children.begin(), children.end());
                                }
                                _starting_nodes = std::move(data_ids);
                                if (_starting_nodes.empty()) {
                                    _starting_nodes.push_back(0xFFFFFFFFFFFFFFFFULL);
                                }
                            } else {
                                _starting_nodes = std::move(coll_ids);
                            }
                            resolved_start = true;
                            best_start_priority = 3;
                            rodsLog(LOG_NOTICE, "L3_BRIDGE: eq_or_like resolved %zu starting nodes for '%s'", _starting_nodes.size(), target_coll.c_str());
                        }
                    }
                }
            } else if (std::regex_match(cond, match, parent_regex)) {
                std::string target_path = unescape_sql_literal(match[1].str());
                int priority = 4;
                if ((inx == COL_COLL_NAME || inx == COL_COLL_PARENT_NAME) && priority > best_start_priority && _catalog != nullptr) {
                    while (target_path.size() > 1 && target_path.back() == '/') {
                        target_path.pop_back();
                    }

                    std::vector<snowflake_id_t> candidate_nodes;
                    std::unordered_set<snowflake_id_t> seen;
                    auto add_node = [&](const std::string& path_str) {
                        snowflake_id_t sid = 0; EntityType type;
                        if (_catalog->resolve_path(path_str, sid, type).ok() && type == EntityType::Collection) {
                            if (seen.insert(sid).second) {
                                candidate_nodes.push_back(sid);
                            }
                        }
                    };

                    irods::experimental::filesystem::path cur_p(target_path);
                    while (!cur_p.empty() && cur_p.string() != "/") {
                        add_node(cur_p.string());
                        cur_p = cur_p.parent_path();
                    }
                    add_node("/");

                    if (!candidate_nodes.empty()) {
                        _starting_nodes = std::move(candidate_nodes);
                        resolved_start = true;
                        best_start_priority = priority;
                        rodsLog(LOG_NOTICE, "L3_BRIDGE: parent_of resolved %zu candidate collection starting nodes for '%s'", _starting_nodes.size(), target_path.c_str());
                    }
                }
            }
        }

        if (!resolved_start && likely_root == "Zone" && _starting_nodes.empty() && _catalog != nullptr) {
            snowflake_id_t zid = _catalog->get_zone_id();
            _starting_nodes.push_back(zid);
            resolved_start = true;
            rodsLog(LOG_NOTICE, "L3_BRIDGE: Resolved Zone starting node 0x%016llx", (unsigned long long)zid);
        } else if (!resolved_start && likely_root == "Resource" && _starting_nodes.empty() && _catalog != nullptr) {
            snowflake_id_t zid = _catalog->get_zone_id();
            auto resc_nodes = _catalog->get_client()->get_neighbors_async(_catalog->get_cluster_id(), zid, "HAS_RESC", 0.0).get();
            _starting_nodes = std::move(resc_nodes);
            resolved_start = true;
            rodsLog(LOG_NOTICE, "L3_BRIDGE: Resolved %zu Resource starting nodes from Zone HAS_RESC", _starting_nodes.size());
        } else if (!resolved_start && (likely_root == "User" || likely_root == "Group") && _starting_nodes.empty() && _catalog != nullptr) {
            snowflake_id_t zid = _catalog->get_zone_id();
            auto user_nodes = _catalog->get_client()->get_neighbors_async(_catalog->get_cluster_id(), zid, "HAS_USER", 0.0).get();
            _starting_nodes = std::move(user_nodes);
            resolved_start = true;
            rodsLog(LOG_NOTICE, "L3_BRIDGE: Resolved %zu %s starting nodes from Zone HAS_USER", _starting_nodes.size(), likely_root.c_str());
        } else if (!resolved_start && likely_root == "Rule" && _starting_nodes.empty() && _catalog != nullptr) {
            snowflake_id_t zid = _catalog->get_zone_id();
            auto rule_nodes = _catalog->get_client()->get_neighbors_async(_catalog->get_cluster_id(), zid, "HAS_RULE", 0.0).get();
            _starting_nodes = std::move(rule_nodes);
            resolved_start = true;
        } else if (!resolved_start && likely_root == "Ticket" && _starting_nodes.empty() && _catalog != nullptr) {
            snowflake_id_t zid = _catalog->get_zone_id();
            auto ticket_nodes = _catalog->get_client()->get_neighbors_async(_catalog->get_cluster_id(), zid, "HAS_TICKET", 0.0).get();
            _starting_nodes = std::move(ticket_nodes);
            resolved_start = true;
            rodsLog(LOG_NOTICE, "L3_BRIDGE: Resolved %zu Ticket starting nodes from Zone HAS_TICKET", _starting_nodes.size());
        }

        // Pass 2: Build conditions
        for (int i = 0; i < _inp->sqlCondInp.len; ++i) {
            int inx = _inp->sqlCondInp.inx[i];
            int pure_inx = get_pure_inx(inx);
            std::string cond(_inp->sqlCondInp.value[i]);
            std::string name = get_col_name(pure_inx);
            
            rodsLog(LOG_NOTICE, "L3_BRIDGE: Raw Condition Received: index=%d (%s), value='%s'", inx, name.c_str(), cond.c_str());

            if (!name.empty()) {
                gq2::column col(name);
                std::smatch match;
                if (std::regex_search(cond, in_clause_regex)) {
                    auto words_begin = std::sregex_iterator(cond.begin(), cond.end(), quoted_literal_regex);
                    auto words_end = std::sregex_iterator();
                    std::vector<std::string> literals;
                    for (std::sregex_iterator it = words_begin; it != words_end; ++it) {
                        literals.push_back(unescape_sql_literal((*it)[1].str()));
                    }
                    if (literals.empty()) {
                        size_t p1 = cond.find('(');
                        size_t p2 = cond.rfind(')');
                        if (p1 != std::string::npos && p2 != std::string::npos && p2 > p1) {
                            std::string inside = cond.substr(p1 + 1, p2 - p1 - 1);
                            std::stringstream ss(inside);
                            std::string token;
                            while (std::getline(ss, token, ',')) {
                                boost::algorithm::trim(token);
                                if (!token.empty()) {
                                    if (token.front() == '\'' && token.back() == '\'') token = token.substr(1, token.size() - 2);
                                    literals.push_back(unescape_sql_literal(token));
                                }
                            }
                        }
                    }
                    if (literals.empty()) {
                        ast.conditions.push_back(gq2::condition(col, gq2::condition_equal("__EMPTY_IN_CLAUSE__")));
                    } else if (literals.size() == 1) {
                        ast.conditions.push_back(gq2::condition(col, gq2::condition_equal(literals[0])));
                    } else {
                        gq2::logical_or or_cond;
                        for (const auto& lit : literals) {
                            or_cond.condition.push_back(gq2::condition(col, gq2::condition_equal(lit)));
                        }
                        ast.conditions.push_back(std::move(or_cond));
                    }
                } else if (cond.find("||") != std::string::npos) {
                    gq2::logical_or or_cond;
                    size_t start = 0;
                    while (start < cond.size()) {
                        size_t pos = cond.find("||", start);
                        std::string sub = (pos == std::string::npos) ? cond.substr(start) : cond.substr(start, pos - start);
                        boost::algorithm::trim(sub);
                        if (!sub.empty()) {
                            std::smatch m;
                            if (std::regex_match(sub, m, not_like_regex)) {
                                or_cond.condition.push_back(gq2::condition(col, gq2::condition_operator_not{gq2::condition_like(extract_literal(m))}));
                            } else if (std::regex_match(sub, m, like_regex)) {
                                or_cond.condition.push_back(gq2::condition(col, gq2::condition_like(extract_literal(m))));
                            } else if (std::regex_match(sub, m, ne_regex)) {
                                or_cond.condition.push_back(gq2::condition(col, gq2::condition_not_equal(extract_literal(m))));
                            } else if (std::regex_match(sub, m, le_regex)) {
                                or_cond.condition.push_back(gq2::condition(col, gq2::condition_less_than_or_equal_to(extract_literal(m))));
                            } else if (std::regex_match(sub, m, ge_regex)) {
                                or_cond.condition.push_back(gq2::condition(col, gq2::condition_greater_than_or_equal_to(extract_literal(m))));
                            } else if (std::regex_match(sub, m, lt_regex)) {
                                or_cond.condition.push_back(gq2::condition(col, gq2::condition_less_than(extract_literal(m))));
                            } else if (std::regex_match(sub, m, gt_regex)) {
                                or_cond.condition.push_back(gq2::condition(col, gq2::condition_greater_than(extract_literal(m))));
                            } else if (std::regex_match(sub, m, eq_regex)) {
                                or_cond.condition.push_back(gq2::condition(col, gq2::condition_equal(extract_literal(m))));
                            } else {
                                or_cond.condition.push_back(gq2::condition(col, gq2::condition_equal(sub)));
                            }
                        }
                        if (pos == std::string::npos) break;
                        start = pos + 2;
                    }
                    if (or_cond.condition.size() == 1) {
                        ast.conditions.push_back(std::move(or_cond.condition.front()));
                    } else if (or_cond.condition.size() > 1) {
                        ast.conditions.push_back(std::move(or_cond));
                    }
                } else if (std::regex_match(cond, match, not_like_regex)) {
                    ast.conditions.push_back(gq2::condition(col, gq2::condition_operator_not{gq2::condition_like(extract_literal(match))}));
                } else if (std::regex_match(cond, match, le_regex)) {
                    ast.conditions.push_back(gq2::condition(col, gq2::condition_less_than_or_equal_to(extract_literal(match))));
                } else if (std::regex_match(cond, match, ge_regex)) {
                    ast.conditions.push_back(gq2::condition(col, gq2::condition_greater_than_or_equal_to(extract_literal(match))));
                } else if (std::regex_match(cond, match, lt_regex)) {
                    ast.conditions.push_back(gq2::condition(col, gq2::condition_less_than(extract_literal(match))));
                } else if (std::regex_match(cond, match, gt_regex)) {
                    ast.conditions.push_back(gq2::condition(col, gq2::condition_greater_than(extract_literal(match))));
                } else if (std::regex_match(cond, match, eq_regex)) {
                    ast.conditions.push_back(gq2::condition(col, gq2::condition_equal(extract_literal(match))));
                } else if (std::regex_match(cond, match, ne_regex)) {
                    ast.conditions.push_back(gq2::condition(col, gq2::condition_not_equal(extract_literal(match))));
                } else if (std::regex_match(cond, match, like_regex)) {
                    ast.conditions.push_back(gq2::condition(col, gq2::condition_like(extract_literal(match))));
                } else if (std::regex_match(cond, match, parent_regex)) {
                    if (_catalog == nullptr) {
                        irods::experimental::filesystem::path p(match[1].str());
                        ast.conditions.push_back(gq2::condition(col, gq2::condition_equal(unescape_sql_literal(p.string()))));
                    } else if (!resolved_start || _starting_nodes.empty()) {
                        ast.conditions.push_back(gq2::condition(col, gq2::condition_equal("__NON_EXISTENT_PARENT_OF_PATH__")));
                    }
                } else {
                    rodsLog(LOG_WARNING, "L3_BRIDGE: Condition on column '%s' was NOT recognized: '%s'", name.c_str(), cond.c_str());
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

        if (_inp->maxRows > 0 && _out->rowCnt == _inp->maxRows) {
            _out->continueInx = _inp->continueInx + _out->rowCnt;
        } else {
            _out->continueInx = 0;
        }

        _out->totalRowCount = _out->rowCnt;

        for (int i = 0; i < _out->attriCnt; ++i) {
            int inx = _inp->selectInp.inx[i];
            int pure_inx = get_pure_inx(inx);
            _out->sqlResult[i].attriInx = inx;
            
            // Standardize on MAX_NAME_LEN for all GenQuery 1 output columns.
            // Many iRODS clients (including iadmin) are sensitive to memory layout 
            // and often assume 2700 byte buffers for all results.
            int col_len = 2700; 
            
            _out->sqlResult[i].len = col_len;
            _out->sqlResult[i].value = (char*)malloc(_out->rowCnt * col_len);
            if (!_out->sqlResult[i].value) throw std::runtime_error("Failed to allocate result buffer");
            memset(_out->sqlResult[i].value, 0, _out->rowCnt * col_len);

            for (int r = 0; r < _out->rowCnt; ++r) {
                std::string val{_results.get_field(r, (size_t)i)};

                // Detect if the column is likely numeric to avoid std::stoll crashes
                bool is_string_col = (pure_inx == COL_DATA_NAME || pure_inx == COL_COLL_NAME || 
                                     pure_inx == COL_USER_NAME || pure_inx == COL_R_RESC_NAME || 
                                     pure_inx == COL_D_DATA_PATH || pure_inx == COL_D_RESC_HIER ||
                                     pure_inx == COL_D_DATA_CHECKSUM || pure_inx == COL_D_OWNER_NAME || 
                                     pure_inx == COL_D_OWNER_ZONE || pure_inx == COL_COLL_OWNER_NAME || 
                                     pure_inx == COL_COLL_OWNER_ZONE || pure_inx == COL_D_RESC_NAME ||
                                     pure_inx == COL_ZONE_NAME || pure_inx == COL_R_ZONE_NAME || pure_inx == COL_DATA_ACCESS_NAME ||
                                     pure_inx == COL_DATA_TOKEN_NAMESPACE || pure_inx == COL_COLL_ACCESS_NAME ||
                                     pure_inx == COL_COLL_TOKEN_NAMESPACE || pure_inx == COL_COLL_PARENT_NAME ||
                                     pure_inx == COL_D_COMMENTS || pure_inx == COL_DATA_TYPE_NAME ||
                                     pure_inx == COL_D_EXPIRY || pure_inx == COL_D_DATA_STATUS ||
                                     pure_inx == COL_DATA_VERSION || pure_inx == COL_D_MAP_ID ||
                                     pure_inx == COL_COLL_INFO1 || pure_inx == COL_COLL_INFO2 ||
                                     pure_inx == COL_USER_TYPE || pure_inx == COL_ZONE_TYPE ||
                                     pure_inx == COL_R_TYPE_NAME || pure_inx == COL_R_CLASS_NAME ||
                                     pure_inx == COL_R_LOC || pure_inx == COL_R_VAULT_PATH ||
                                     pure_inx == COL_COLL_TYPE || pure_inx == COL_DATA_ACCESS_TYPE ||
                                     pure_inx == COL_COLL_ACCESS_TYPE || pure_inx == COL_TICKET_STRING ||
                                     pure_inx == COL_TICKET_TYPE || pure_inx == COL_TICKET_DATA_NAME ||
                                     pure_inx == COL_TICKET_COLL_NAME || pure_inx == COL_TICKET_OWNER_NAME ||
                                     pure_inx == COL_TICKET_OBJECT_TYPE || pure_inx == COL_TICKET_DATA_COLL_NAME ||
                                     pure_inx == COL_TICKET_OWNER_ZONE || pure_inx == COL_TICKET_ALLOWED_HOST ||
                                     pure_inx == COL_TICKET_ALLOWED_USER_NAME || pure_inx == COL_TICKET_ALLOWED_GROUP_NAME ||
                                     pure_inx == COL_META_DATA_ATTR_NAME || pure_inx == COL_META_DATA_ATTR_VALUE ||
                                     pure_inx == COL_META_DATA_ATTR_UNITS || pure_inx == COL_META_COLL_ATTR_NAME ||
                                     pure_inx == COL_META_COLL_ATTR_VALUE || pure_inx == COL_META_COLL_ATTR_UNITS ||
                                     pure_inx == COL_META_RESC_ATTR_NAME || pure_inx == COL_META_RESC_ATTR_VALUE ||
                                     pure_inx == COL_META_RESC_ATTR_UNITS || pure_inx == COL_META_USER_ATTR_NAME ||
                                     pure_inx == COL_META_USER_ATTR_VALUE || pure_inx == COL_META_USER_ATTR_UNITS ||
                                     pure_inx == COL_USER_GROUP_NAME || pure_inx == COL_R_RESC_INFO ||
                                     pure_inx == COL_R_RESC_COMMENT || pure_inx == COL_R_RESC_CHILDREN ||
                                     pure_inx == COL_R_RESC_CONTEXT || pure_inx == COL_R_RESC_PARENT ||
                                     pure_inx == COL_R_RESC_PARENT_CONTEXT || pure_inx == COL_R_RESC_STATUS ||
                                     pure_inx == COL_RULE_NAME || pure_inx == COL_RULE_BODY ||
                                     pure_inx == COL_RULE_OWNER_NAME || pure_inx == COL_DVM_BASE_NAME ||
                                     pure_inx == COL_DVM_EXT_VAR_NAME || pure_inx == COL_DVM_INT_MAP_PATH ||
                                     pure_inx == COL_FNM_BASE_NAME || pure_inx == COL_FNM_EXT_FUNC_NAME ||
                                     pure_inx == COL_FNM_INT_FUNC_NAME || pure_inx == COL_AUDIT_COMMENT ||
                                     pure_inx == COL_SL_HOST_NAME || pure_inx == COL_SL_RESC_NAME ||
                                     pure_inx == COL_COLL_USER_NAME || pure_inx == COL_COLL_USER_ZONE ||
                                     pure_inx == COL_DATA_USER_NAME || pure_inx == COL_DATA_USER_ZONE ||
                                     pure_inx == COL_RESC_USER_NAME || pure_inx == COL_RESC_USER_ZONE ||
                                     pure_inx == COL_RULE_EXEC_NAME || pure_inx == COL_RULE_EXEC_REI_FILE_PATH ||
                                     pure_inx == COL_RULE_EXEC_USER_NAME || pure_inx == COL_RULE_EXEC_ADDRESS ||
                                     pure_inx == COL_RULE_EXEC_TIME || pure_inx == COL_RULE_EXEC_FREQUENCY ||
                                     pure_inx == COL_RULE_EXEC_ESTIMATED_EXE_TIME || pure_inx == COL_RULE_EXEC_NOTIFICATION_ADDR ||
                                     pure_inx == COL_RULE_EXEC_LAST_EXE_TIME || pure_inx == COL_RULE_EXEC_STATUS ||
                                     pure_inx == COL_RULE_EXEC_CONTEXT || pure_inx == COL_RULE_EXEC_LOCK_HOST ||
                                     pure_inx == COL_RULE_EXEC_LOCK_HOST_PID || pure_inx == COL_RULE_EXEC_LOCK_TIME);

                if (val.empty() && pure_inx == COL_COLL_TYPE) {
                    val = "";
                } else if (val.empty() && pure_inx == COL_D_EXPIRY) {
                    val = "00000000000";
                } else if ((val.empty() || val == "00000000000") && pure_inx == COL_TICKET_EXPIRY_TS) {
                    val = "0";
                } else if (val.empty() && pure_inx == COL_RULE_EXEC_PRIORITY) {
                    val = "5";
                } else if (val.empty()) {
                    if (is_string_col) {
                        val = ""; 
                    } else {
                        val = "0"; // Conservative default for all non-string columns
                    }
                } else if (pure_inx == COL_COLL_TYPE && val == "collection") {
                    val = "";
                }

                // Map L3KVG permission labels to numeric strings
                if (pure_inx == COL_DATA_ACCESS_TYPE || pure_inx == COL_COLL_ACCESS_TYPE || pure_inx == COL_DATA_ACCESS_NAME || pure_inx == COL_COLL_ACCESS_NAME) {
                    rodsLog(LOG_NOTICE, "L3_BRIDGE: Mapping permission column %d value [%s]", pure_inx, val.c_str());
                    
                    // Handle admin: prefix
                    if (val.starts_with("admin:")) {
                        val = val.substr(6);
                    }

                    if (val == "own") {
                        if (pure_inx == COL_DATA_ACCESS_TYPE || pure_inx == COL_COLL_ACCESS_TYPE) val = "1200";
                    }
                    else if (val == "write" || val == "modify_object") {
                        if (pure_inx == COL_DATA_ACCESS_TYPE || pure_inx == COL_COLL_ACCESS_TYPE) val = "1100";
                        else if (pure_inx == COL_DATA_ACCESS_NAME || pure_inx == COL_COLL_ACCESS_NAME) val = "modify_object";
                    }
                    else if (val == "read" || val == "read_object") {
                        if (pure_inx == COL_DATA_ACCESS_TYPE || pure_inx == COL_COLL_ACCESS_TYPE) val = "1050";
                        else if (pure_inx == COL_DATA_ACCESS_NAME || pure_inx == COL_COLL_ACCESS_NAME) val = "read_object";
                    }
                    else if (val == "null" || val.empty()) {
                        if (pure_inx == COL_DATA_ACCESS_TYPE || pure_inx == COL_COLL_ACCESS_TYPE) val = "1000";
                        else if (pure_inx == COL_DATA_ACCESS_NAME || pure_inx == COL_COLL_ACCESS_NAME) val = "null";
                    }
                }

                if (r < 5) {
                    rodsLog(LOG_NOTICE, "L3_BRIDGE: Packing Col %d (pure: %d) Row %d: [%s]", inx, pure_inx, r, val.c_str());
                }
                strncpy(&_out->sqlResult[i].value[r * col_len], val.c_str(), col_len - 1);
            }
        }
    }

} // namespace irods::catalog::bridge
