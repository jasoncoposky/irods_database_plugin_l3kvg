#include "irods/catalog/gq2_compiler.hpp"
#include "irods/rodsLog.h"
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <string>
#include <nlohmann/json.hpp>
#include <boost/variant.hpp>
#include "irods/rodsGenQuery.h"

namespace irods::catalog::compiler {

    using json = nlohmann::json;
    namespace gq2 = irods::experimental::genquery2;
    using Direction = Gq2ToL3kvgCompiler::PathStep::Direction;

    const std::unordered_map<int, GraphMap> COLUMN_MAP = {
        {COL_D_DATA_ID,        {"DataObject", "id"}},
        {COL_D_COLL_ID,        {"Collection", "id"}},
        {COL_DATA_NAME,        {"DataObject", "n"}},
        {COL_DATA_SIZE,        {"DataObject", "s"}},
        {COL_DATA_MODE,        {"DataObject", "mode"}},
        {COL_D_RESC_NAME,      {"Resource", "n"}},
        {COL_D_OWNER_NAME,     {"DataObject", "o"}},
        {COL_D_OWNER_ZONE,     {"DataObject", "z"}},
        {COL_D_CREATE_TIME,    {"DataObject", "ct"}},
        {COL_D_MODIFY_TIME,    {"DataObject", "mt"}},
        {COL_D_DATA_CHECKSUM,  {"Replica", "cs"}},
        {COL_D_DATA_STATUS,    {"DataObject", "st"}},


        {COL_COLL_ID,          {"Collection", "id"}},
        {COL_COLL_NAME,        {"Collection", "n"}},
        {COL_COLL_OWNER_NAME,  {"Collection", "o"}},
        {COL_COLL_OWNER_ZONE,  {"Collection", "z"}},
        {COL_COLL_INHERITANCE, {"Collection", "i"}},
        {COL_COLL_COMMENTS,    {"Collection", "m"}},
        {COL_COLL_CREATE_TIME, {"Collection", "ct"}},
        {COL_COLL_MODIFY_TIME, {"Collection", "mt"}},
        {COL_COLL_TYPE,        {"Collection", "t"}},
        {COL_COLL_INFO1,       {"Collection", "c1"}},
        {COL_COLL_INFO2,       {"Collection", "c2"}},

        {COL_USER_ID,          {"User", "id"}},
        {COL_USER_NAME,        {"User", "n"}},
        {COL_USER_TYPE,        {"User", "t"}},
        {COL_USER_ZONE,        {"User", "z"}},
        {COL_USER_DN,          {"User", "d"}},
        {COL_USER_CREATE_TIME, {"User", "ct"}},
        {COL_USER_MODIFY_TIME, {"User", "mt"}},
        {COL_USER_GROUP_ID,    {"Group", "id"}},
        {COL_USER_GROUP_NAME,  {"Group", "n"}},

        {COL_R_RESC_ID,        {"Resource", "id"}},
        {COL_R_RESC_NAME,      {"Resource", "n"}},
        {COL_R_ZONE_NAME,      {"Resource", "z"}},
        {COL_R_TYPE_NAME,      {"Resource", "t"}},
        {COL_R_CLASS_NAME,     {"Resource", "c"}},
        {COL_R_LOC,            {"Resource", "l"}},
        {COL_R_VAULT_PATH,     {"Resource", "v"}},
        {COL_R_FREE_SPACE,     {"Resource", "f"}},
        {COL_R_RESC_INFO,      {"Resource", "i"}},
        {COL_R_RESC_COMMENT,   {"Resource", "m"}},
        {COL_R_CREATE_TIME,    {"Resource", "ct"}},
        {COL_R_MODIFY_TIME,    {"Resource", "mt"}},

        {COL_D_RESC_ID,        {"Replica", "rid"}},
        {COL_D_DATA_PATH,      {"Replica", "p"}},
        {COL_DATA_REPL_NUM,    {"Replica", "rn"}},
        {COL_D_REPL_STATUS,    {"Replica", "st"}},
        {COL_D_RESC_HIER,      {"Replica", "rh"}},

        {COL_ZONE_ID,          {"Zone", "id"}},
        {COL_ZONE_NAME,        {"Zone", "n"}},
        {COL_ZONE_TYPE,        {"Zone", "t"}},
        {COL_ZONE_CONNECTION,  {"Zone", "c"}},
        {COL_ZONE_COMMENT,     {"Zone", "m"}},
        {COL_DATA_ACCESS_TYPE, {"Access", "l"}},
        {COL_DATA_ACCESS_NAME, {"Access", "l"}},
        {COL_COLL_ACCESS_TYPE, {"Access", "l"}},
        {COL_COLL_ACCESS_NAME, {"Access", "l"}},
        {COL_DATA_TOKEN_NAMESPACE, {"Access", "t"}},
        {COL_COLL_TOKEN_NAMESPACE, {"Access", "t"}},
        {COL_DATA_ACCESS_DATA_ID, {"DataObject", "id"}},
        {COL_COLL_ACCESS_COLL_ID, {"Collection", "id"}},
        {COL_DATA_ACCESS_USER_ID, {"User", "id"}},
        {COL_COLL_ACCESS_USER_ID, {"User", "id"}}
    };

    const std::unordered_map<std::string_view, GraphMap> COLUMN_NAME_MAP = {
        {"DATA_ID",           {"DataObject", "id"}},
        {"DATA_COLL_ID",      {"Collection", "id"}},
        {"DATA_NAME",         {"DataObject", "n"}},
        {"DATA_SIZE",         {"DataObject", "s"}},
        {"DATA_MODE",         {"DataObject", "mode"}},
        {"DATA_VERSION",      {"DataObject", "v"}},
        {"DATA_TYPE_NAME",    {"DataObject", "t"}},
        {"DATA_OWNER_NAME",   {"DataObject", "o"}},
        {"DATA_OWNER_ZONE",   {"DataObject", "z"}},
        {"DATA_CREATE_TIME",  {"DataObject", "ct"}},
        {"DATA_MODIFY_TIME",  {"DataObject", "mt"}},
        {"DATA_CHECKSUM",     {"Replica", "cs"}},
        {"DATA_EXPIRY",       {"DataObject", "ex"}},
        {"DATA_MAP_ID",       {"DataObject", "m"}},
        {"DATA_COMMENTS",     {"DataObject", "c"}},
        {"DATA_RESC_NAME",    {"Resource", "n"}},
        {"D_RESC_NAME",       {"Resource", "n"}},
        {"DATA_STATUS",       {"DataObject", "st"}},
        {"DATA_DATA_STATUS",  {"DataObject", "st"}},

        {"COLL_ID",           {"Collection", "id"}},
        {"COLL_NAME",         {"Collection", "n"}},
        {"COLL_PARENT_NAME",  {"Collection", "pn"}},
        {"COLL_OWNER_NAME",   {"Collection", "o"}},
        {"COLL_OWNER_ZONE",   {"Collection", "z"}},
        {"COLL_MAP_ID",       {"Collection", "m"}},
        {"COLL_INHERITANCE",  {"Collection", "i"}},
        {"COLL_COMMENTS",     {"Collection", "m"}},
        {"COLL_CREATE_TIME",  {"Collection", "ct"}},
        {"COLL_MODIFY_TIME",  {"Collection", "mt"}},
        {"COLL_TYPE",         {"Collection", "t"}},
        {"COLL_INFO1",        {"Collection", "c1"}},
        {"COLL_INFO2",        {"Collection", "c2"}},

        {"USER_ID",           {"User", "id"}},
        {"USER_NAME",         {"User", "n"}},
        {"USER_TYPE",         {"User", "t"}},
        {"USER_ZONE",         {"User", "z"}},
        {"USER_INFO",         {"User", "i"}},
        {"USER_COMMENT",      {"User", "c"}},
        {"USER_CREATE_TIME",  {"User", "ct"}},
        {"USER_MODIFY_TIME",  {"User", "mt"}},
        {"USER_DN",           {"User", "d"}},

        {"RESC_ID",           {"Resource", "id"}},
        {"RESC_NAME",         {"Resource", "n"}},
        {"RESC_ZONE_NAME",    {"Resource", "z"}},
        {"ZONE_NAME",         {"Resource", "z"}},
        {"RESC_TYPE_NAME",    {"Resource", "t"}},
        {"RESC_CLASS_NAME",   {"Resource", "c"}},
        {"RESC_LOC",          {"Resource", "l"}},
        {"RESC_VAULT_PATH",   {"Resource", "v"}},
        {"RESC_FREE_SPACE",   {"Resource", "f"}},
        {"RESC_FREE_SPACE_TIME", {"Resource", "ft"}},
        {"RESC_INFO",         {"Resource", "i"}},
        {"RESC_COMMENT",      {"Resource", "m"}},
        {"RESC_STATUS",       {"Resource", "s"}},
        {"RESC_CHILDREN",     {"Resource", "ch"}},
        {"RESC_CONTEXT",      {"Resource", "cx"}},
        {"RESC_PARENT",       {"Resource", "p"}},
        {"RESC_PARENT_CONTEXT", {"Resource", "pc"}},
        {"RESC_CREATE_TIME",  {"Resource", "ct"}},
        {"RESC_MODIFY_TIME",  {"Resource", "mt"}},

        {"ZONE_ID",           {"Zone", "id"}},
        {"ZONE_NAME",         {"Zone", "n"}},
        {"ZONE_TYPE",         {"Zone", "t"}},
        {"ZONE_CONNECTION",   {"Zone", "c"}},
        {"ZONE_COMMENT",      {"Zone", "m"}},
        {"ZONE_CREATE_TIME",  {"Zone", "ct"}},
        {"ZONE_MODIFY_TIME",  {"Zone", "mt"}},

        {"META_DATA_ATTR_NAME",  {"Metadata", "a"}},
        {"META_DATA_ATTR_VALUE", {"Metadata", "v"}},
        {"META_DATA_ATTR_UNITS", {"Metadata", "u"}},
        {"META_DATA_ATTR_ID",    {"Metadata", "id"}},
        {"META_COLL_ATTR_NAME",  {"Metadata", "a"}},
        {"META_COLL_ATTR_VALUE", {"Metadata", "v"}},
        {"META_COLL_ATTR_UNITS", {"Metadata", "u"}},
        {"META_COLL_ATTR_ID",    {"Metadata", "id"}},
        {"META_RESC_ATTR_NAME",  {"Metadata", "a"}},
        {"META_RESC_ATTR_VALUE", {"Metadata", "v"}},
        {"META_RESC_ATTR_UNITS", {"Metadata", "u"}},
        {"META_RESC_ATTR_ID",    {"Metadata", "id"}},
        {"META_USER_ATTR_NAME",  {"Metadata", "a"}},
        {"META_USER_ATTR_VALUE", {"Metadata", "v"}},
        {"META_USER_ATTR_UNITS", {"Metadata", "u"}},
        {"META_USER_ATTR_ID",    {"Metadata", "id"}},

        {"USER_GROUP_ID",        {"Group", "id"}},
        {"USER_GROUP_NAME",      {"Group", "n"}},

        {"D_RESC_ID",      {"Replica", "rid"}},
        {"DATA_REPL_NUM",  {"Replica", "rn"}},
        {"DATA_PATH",      {"Replica", "p"}},
        {"DATA_REPL_STATUS",{"Replica", "st"}},
        {"D_RESC_HIER",    {"Replica", "rh"}},

        {"DATA_ACCESS_NAME",      {"Access", "l"}},
        {"DATA_ACCESS_TYPE",      {"Access", "l"}},
        {"COLL_ACCESS_NAME",      {"Access", "l"}},
        {"COLL_ACCESS_TYPE",      {"Access", "l"}},
        {"DATA_TOKEN_NAMESPACE",  {"Access", "t"}},
        {"COLL_TOKEN_NAMESPACE",  {"Access", "t"}},

        {"TICKET_ID",         {"Ticket", "id"}},
        {"TICKET_STRING",     {"Ticket", "s"}},
        {"TICKET_TYPE",       {"Ticket", "t"}},
        {"TICKET_USES_LIMIT", {"Ticket", "ul"}},
        {"TICKET_USES_COUNT", {"Ticket", "uc"}},
        {"TICKET_EXPIRY_TS",  {"Ticket", "ex"}},
        {"TICKET_CREATE_TIME",{"Ticket", "ct"}},
        {"TICKET_MODIFY_TIME",{"Ticket", "mt"}},
        {"TICKET_WRITE_FILE_COUNT", {"Ticket", "wfc"}},
        {"TICKET_WRITE_FILE_LIMIT", {"Ticket", "wfl"}},
        {"TICKET_WRITE_BYTE_COUNT", {"Ticket", "wbc"}},
        {"TICKET_WRITE_BYTE_LIMIT", {"Ticket", "wbl"}},
        {"TICKET_DATA_NAME",  {"DataObject", "n"}},
        {"TICKET_COLL_NAME",  {"Collection", "n"}},
        {"TICKET_OWNER_NAME", {"User", "n"}},
        {"TICKET_ALLOWED_HOST_TICKET_ID", {"Ticket", "id"}},
        {"TICKET_ALLOWED_HOST",           {"Ticket", "h"}},
        {"TICKET_ALLOWED_USER_TICKET_ID", {"Ticket", "id"}},
        {"TICKET_ALLOWED_USER_NAME",      {"User", "n"}},
        {"TICKET_ALLOWED_GROUP_TICKET_ID",{"Ticket", "id"}},
        {"TICKET_ALLOWED_GROUP_NAME",     {"User", "n"}},

        {"MSRVC_ID",          {"MSRVC", "id"}},
        {"MSRVC_NAME",        {"MSRVC", "n"}},
        {"MSRVC_SIGNATURE",   {"MSRVC", "s"}},
        {"MSRVC_DOXYGEN",     {"MSRVC", "d"}},
        {"MSRVC_VARIATIONS",  {"MSRVC", "v"}},
        {"MSRVC_STATUS",      {"MSRVC", "st"}},
        {"MSRVC_OWNER_NAME",  {"User", "n"}},
        {"MSRVC_OWNER_ZONE",  {"User", "z"}},
        {"MSRVC_COMMENT",     {"MSRVC", "c"}},
        {"MSRVC_CREATE_TIME", {"MSRVC", "ct"}},
        {"MSRVC_MODIFY_TIME", {"MSRVC", "mt"}},
        {"MSRVC_VERSION",     {"MSRVC", "ver"}},
        {"MSRVC_HOST",        {"MSRVC", "h"}},
        {"MSRVC_LOCATION",    {"MSRVC", "l"}},
        {"MSRVC_LANGUAGE",    {"MSRVC", "lang"}},
        {"MSRVC_TYPE_NAME",   {"MSRVC", "t"}},
        {"MSRVC_MODULE_NAME", {"MSRVC", "mod"}},

        {"MSRVC_VER_OWNER_NAME", {"MSRVC", "vo"}},
        {"MSRVC_VER_OWNER_ZONE", {"MSRVC", "vz"}},
        {"MSRVC_VER_COMMENT",    {"MSRVC", "vc"}},
        {"MSRVC_VER_CREATE_TIME",{"MSRVC", "vct"}},
        {"MSRVC_VER_MODIFY_TIME",{"MSRVC", "vmt"}},

        {"MSRVC_ACCESS_TYPE",      {"Access", "l"}},
        {"MSRVC_ACCESS_NAME",      {"User", "n"}},
        {"MSRVC_TOKEN_NAMESPACE",  {"Access", "t"}},

        {"AUDIT_OBJ_ID",      {"Audit", "oid"}},
        {"AUDIT_USER_ID",     {"Audit", "uid"}},
        {"AUDIT_ACTION_ID",   {"Audit", "aid"}},
        {"AUDIT_COMMENT",     {"Audit", "m"}},
        {"AUDIT_CREATE_TIME", {"Audit", "ct"}},
        {"AUDIT_MODIFY_TIME", {"Audit", "mt"}},

        {"SL_HOST_NAME",      {"ServerLoad", "h"}},
        {"SL_RESC_NAME",      {"ServerLoad", "rn"}},
        {"SL_CPU_USED",       {"ServerLoad", "cpu"}},
        {"SL_MEM_USED",       {"ServerLoad", "mem"}},
        {"SL_SWAP_USED",      {"ServerLoad", "swp"}},
        {"SL_RUNQ_LOAD",      {"ServerLoad", "runq"}},
        {"SL_DISK_SPACE",     {"ServerLoad", "dsk"}},
        {"SL_NET_INPUT",      {"ServerLoad", "neti"}},
        {"SL_NET_OUTPUT",     {"ServerLoad", "neto"}},
        {"SL_CREATE_TIME",    {"ServerLoad", "ct"}},

        {"RULE_ID",           {"Rule", "id"}},
        {"RULE_NAME",         {"Rule", "n"}},
        {"RULE_BODY",         {"Rule", "b"}},
        {"RULE_OWNER_NAME",   {"User", "n"}},
        {"RULE_CREATE_TIME",  {"Rule", "ct"}},
        {"RULE_MODIFY_TIME",  {"Rule", "mt"}},

        {"DVM_ID",            {"DVM", "id"}},
        {"DVM_BASE_NAME",     {"DVM", "bn"}},
        {"DVM_EXT_VAR_NAME",  {"DVM", "ev"}},
        {"DVM_INT_MAP_PATH",  {"DVM", "im"}},

        {"FNM_ID",            {"FNM", "id"}},
        {"FNM_BASE_NAME",     {"FNM", "bn"}},
        {"FNM_EXT_FUNC_NAME", {"FNM", "ef"}},
        {"FNM_INT_FUNC_NAME", {"FNM", "if"}},

        {"QUOTA_USER_ID",     {"Quota", "uid"}},
        {"QUOTA_RESC_NAME",   {"Resource", "n"}},
        {"QUOTA_LIMIT",       {"Quota", "l"}},
        {"QUOTA_OVER",        {"Quota", "o"}},
        {"QUOTA_USAGE",       {"Quota", "u"}}
    };

    struct InternalRouteKey {
        std::string source;
        std::string target;
        bool operator==(const InternalRouteKey& o) const { return source == o.source && target == o.target; }
    };
    struct InternalRouteKeyHash {
        size_t operator()(const InternalRouteKey& k) const { return std::hash<std::string>{}(k.source) ^ std::hash<std::string>{}(k.target); }
    };

    static const std::unordered_map<InternalRouteKey, std::vector<Gq2ToL3kvgCompiler::PathStep>, InternalRouteKeyHash> ROUTING_TABLE = {
        {{"DataObject", "Collection"}, {{Direction::In, "CONTAINS", "Collection"}}},
        {{"DataObject", "Resource"},   {{Direction::Out, "HAS_REPLICA", "Replica"}, {Direction::Out, "STAYING_AT", "Resource"}}},
        {{"DataObject", "Metadata"},   {{Direction::Out, "ANNOTATED_WITH", "Metadata"}}},
        {{"DataObject", "Replica"},    {{Direction::Out, "HAS_REPLICA", "Replica"}}},
        {{"DataObject", "Access"},     {{Direction::In, "FOR_OBJECT", "Access"}}},
        {{"DataObject", "User"},       {{Direction::In, "FOR_OBJECT", "Access"}, {Direction::In, "HAS_ACCESS", "User"}}},
        {{"DataObject", "Group"},      {{Direction::In, "FOR_OBJECT", "Access"}, {Direction::In, "HAS_ACCESS", "User"}, {Direction::Out, "MEMBER_OF", "Group"}}},
        {{"DataObject", "Zone"},       {{Direction::Out, "HAS_REPLICA", "Replica"}, {Direction::Out, "STAYING_AT", "Resource"}, {Direction::In, "HAS_RESC", "Zone"}}},
        {{"Collection", "DataObject"}, {{Direction::Out, "CONTAINS", "DataObject"}}},
        {{"Collection", "Resource"},   {{Direction::Out, "CONTAINS", "DataObject"}, {Direction::Out, "HAS_REPLICA", "Replica"}, {Direction::Out, "STAYING_AT", "Resource"}}},
        {{"Collection", "Metadata"},   {{Direction::Out, "ANNOTATED_WITH", "Metadata"}}},
        {{"Collection", "Collection"}, {{Direction::Out, "CONTAINS", "Collection"}}},
        {{"Collection", "Access"},     {{Direction::In, "FOR_OBJECT", "Access"}}},
        {{"Collection", "User"},       {{Direction::In, "FOR_OBJECT", "Access"}, {Direction::In, "HAS_ACCESS", "User"}}},
        {{"Collection", "Group"},      {{Direction::In, "FOR_OBJECT", "Access"}, {Direction::In, "HAS_ACCESS", "User"}, {Direction::Out, "MEMBER_OF", "Group"}}},
        {{"Collection", "Zone"},       {{Direction::In, "HAS_ROOT_COLL", "Zone"}}},
        {{"User", "Access"},           {{Direction::Out, "HAS_ACCESS", "Access"}}},
        {{"User", "DataObject"},       {{Direction::Out, "HAS_ACCESS", "Access"}, {Direction::Out, "FOR_OBJECT", "DataObject"}}},
        {{"User", "Collection"},       {{Direction::Out, "HAS_ACCESS", "Access"}, {Direction::Out, "FOR_OBJECT", "Collection"}}},
        {{"User", "Resource"},         {{Direction::Out, "HAS_ACCESS", "Access"}, {Direction::Out, "FOR_OBJECT", "DataObject"}, {Direction::Out, "HAS_REPLICA", "Replica"}, {Direction::Out, "STAYING_AT", "Resource"}}},
        {{"User", "Zone"},             {{Direction::In, "HAS_USER", "Zone"}}},
        {{"User", "Group"},            {{Direction::Out, "MEMBER_OF", "Group"}}},
        {{"Group", "User"},            {{Direction::In, "MEMBER_OF", "User"}}},
        {{"Group", "DataObject"},      {{Direction::In, "MEMBER_OF", "User"}, {Direction::Out, "HAS_ACCESS", "Access"}, {Direction::Out, "FOR_OBJECT", "DataObject"}}},
        {{"Group", "Collection"},      {{Direction::In, "MEMBER_OF", "User"}, {Direction::Out, "HAS_ACCESS", "Access"}, {Direction::Out, "FOR_OBJECT", "Collection"}}},
        {{"Access", "User"},           {{Direction::In, "HAS_ACCESS", "User"}}},
        {{"Access", "DataObject"},     {{Direction::Out, "FOR_OBJECT", "DataObject"}}},
        {{"Access", "Collection"},     {{Direction::Out, "FOR_OBJECT", "Collection"}}},
        {{"Metadata", "DataObject"},   {{Direction::In, "ANNOTATED_WITH", "DataObject"}}},
        {{"Metadata", "Collection"},   {{Direction::In, "ANNOTATED_WITH", "Collection"}}},
        {{"Metadata", "User"},         {{Direction::In, "ANNOTATED_WITH", "User"}}},
        {{"Metadata", "Resource"},     {{Direction::In, "ANNOTATED_WITH", "Resource"}}},
        {{"Resource", "DataObject"},   {{Direction::In, "STAYING_AT", "Replica"}, {Direction::In, "HAS_REPLICA", "DataObject"}}},
        {{"Resource", "Replica"},      {{Direction::In, "STAYING_AT", "Replica"}}},
        {{"Resource", "Zone"},         {{Direction::In, "HAS_RESC", "Zone"}}},
        {{"Zone", "Collection"},       {{Direction::Out, "HAS_ROOT_COLL", "Collection"}}},
        {{"Zone", "User"},             {{Direction::Out, "HAS_USER", "User"}}},
        {{"Zone", "Resource"},         {{Direction::Out, "HAS_RESC", "Resource"}}},
        {{"DataObject", "Ticket"},     {{Direction::In, "FOR_OBJECT", "Ticket"}}},
        {{"Collection", "Ticket"},     {{Direction::In, "FOR_OBJECT", "Ticket"}}},
        {{"User", "Ticket"},           {{Direction::In, "OWNED_BY", "Ticket"}}},
        {{"User", "Quota"},           {{Direction::In, "LIMITS", "Quota"}}},
        {{"User", "Audit"},           {{Direction::In, "BY_USER", "Audit"}}},
        {{"User", "MSRVC"},           {{Direction::In, "OWNED_BY", "MSRVC"}}},
        {{"DataObject", "Audit"},      {{Direction::In, "AUDIT_OF", "Audit"}}},
        {{"Ticket", "DataObject"},     {{Direction::Out, "FOR_OBJECT", "DataObject"}}},
        {{"Ticket", "Collection"},     {{Direction::Out, "FOR_OBJECT", "Collection"}}},
        {{"Ticket", "User"},           {{Direction::Out, "OWNED_BY", "User"}}},
        {{"Audit", "DataObject"},      {{Direction::Out, "AUDIT_OF", "DataObject"}}},
        {{"Audit", "User"},            {{Direction::Out, "BY_USER", "User"}}},
        {{"ServerLoad", "Resource"},   {{Direction::Out, "LOAD_ON", "Resource"}}},
        {{"Rule", "User"},             {{Direction::Out, "OWNED_BY", "User"}}},
        {{"Quota", "User"},            {{Direction::Out, "LIMITS", "User"}}},
        {{"Quota", "Resource"},        {{Direction::Out, "ON_RESC", "Resource"}}},
        {{"MSRVC", "User"},            {{Direction::Out, "OWNED_BY", "User"}}}
    };

    struct pc_visitor : public boost::static_visitor<std::pair<int, std::string>> {
        std::pair<int, std::string> operator()(const irods::experimental::genquery2::condition_equal& c) const { return {0, c.string_literal}; }
        std::pair<int, std::string> operator()(const irods::experimental::genquery2::condition_not_equal& c) const { return {1, c.string_literal}; }
        std::pair<int, std::string> operator()(const irods::experimental::genquery2::condition_greater_than& c) const { return {2, c.string_literal}; }
        std::pair<int, std::string> operator()(const irods::experimental::genquery2::condition_greater_than_or_equal_to& c) const { return {3, c.string_literal}; }
        std::pair<int, std::string> operator()(const irods::experimental::genquery2::condition_less_than& c) const { return {4, c.string_literal}; }
        std::pair<int, std::string> operator()(const irods::experimental::genquery2::condition_less_than_or_equal_to& c) const { return {5, c.string_literal}; }
        std::pair<int, std::string> operator()(const irods::experimental::genquery2::condition_like& c) const { return {6, c.string_literal}; }
        template<typename T> std::pair<int, std::string> operator()(const T&) const { return {0, ""}; }
    };

    struct condition_visitor : public boost::static_visitor<void> {
        Gq2ToL3kvgCompiler* compiler;
        json& j_filters;
        condition_visitor(Gq2ToL3kvgCompiler* c, json& jf) : compiler(c), j_filters(jf) {}

        void operator()(const irods::experimental::genquery2::condition& c) const {
            std::string col_name;
            if (auto* col = std::get_if<irods::experimental::genquery2::column>(&c.lhs)) col_name = col->name;
            else if (auto* func = std::get_if<irods::experimental::genquery2::function>(&c.lhs)) col_name = func->name;
            auto it = COLUMN_NAME_MAP.find(col_name);
            if (it == COLUMN_NAME_MAP.end()) return;
            compiler->add_target_type(it->second.node_type);
            pc_visitor pcv;
            auto pc = boost::apply_visitor(pcv, c.expression);
            j_filters.push_back({{"alias", it->second.node_type}, {"key", it->second.bson_key}, {"op", pc.first}, {"value", pc.second}});
        }

        void operator()(const irods::experimental::genquery2::logical_and& l) const { for(const auto& c : l.condition) boost::apply_visitor(*this, c); }
        void operator()(const irods::experimental::genquery2::logical_or& l) const { for(const auto& c : l.condition) boost::apply_visitor(*this, c); }
        void operator()(const irods::experimental::genquery2::logical_grouping& l) const { for(const auto& c : l.conditions) boost::apply_visitor(*this, c); }
        void operator()(const irods::experimental::genquery2::logical_not& l) const { for(const auto& c : l.condition) boost::apply_visitor(*this, c); }
    };

    struct projection_visitor : public boost::static_visitor<void> {
        Gq2ToL3kvgCompiler* compiler;
        json& j_projs;
        mutable int col_idx = 0;
        projection_visitor(Gq2ToL3kvgCompiler* c, json& jp) : compiler(c), j_projs(jp) {}

        void operator()(const irods::experimental::genquery2::column& col) const {
            auto it = COLUMN_NAME_MAP.find(col.name);
            if (it != COLUMN_NAME_MAP.end()) {
                compiler->add_target_type(it->second.node_type);
                j_projs.push_back({{"alias", it->second.node_type}, {"property", it->second.bson_key}, {"agg", 0}, {"as", "idx_" + std::to_string(col_idx++)}});
            } else {
                // Return a constant dummy value to maintain column ordering
                j_projs.push_back({{"alias", compiler->get_entry_type()}, {"property", "null_prop"}, {"agg", 0}, {"as", "idx_" + std::to_string(col_idx++)}});
            }
        }

        void operator()(const irods::experimental::genquery2::function& func) const {
            int agg = 0;
            if (func.name == "COUNT") agg = 1;
            else if (func.name == "SUM") agg = 2;
            else if (func.name == "AVG") agg = 3;
            else if (func.name == "MIN") agg = 4;
            else if (func.name == "MAX") agg = 5;

            for (const auto& arg : func.arguments) {
                if (auto* col = std::get_if<irods::experimental::genquery2::column>(&arg)) {
                    auto it = COLUMN_NAME_MAP.find(col->name);
                    if (it != COLUMN_NAME_MAP.end()) {
                        compiler->add_target_type(it->second.node_type);
                        j_projs.push_back({{"alias", it->second.node_type}, {"property", it->second.bson_key}, {"agg", agg}, {"as", "idx_" + std::to_string(col_idx++)}});
                    } else {
                        j_projs.push_back({{"alias", compiler->get_entry_type()}, {"property", "null_prop"}, {"agg", agg}, {"as", "idx_" + std::to_string(col_idx++)}});
                    }
                }
            }
        }
    };

    std::string Gq2ToL3kvgCompiler::compile(const irods::experimental::genquery2::select& ast, std::string_view override_root_alias) {
        rodsLog(LOG_NOTICE, "L3_COMPILER: Entering compile()");

        if (!override_root_alias.empty()) {
            entry_node_type_ = override_root_alias;
            rodsLog(LOG_NOTICE, "L3_COMPILER: Using override root alias: %s", entry_node_type_.c_str());
        } else {
            struct anchor_visitor : public boost::static_visitor<void> {
                 Gq2ToL3kvgCompiler* compiler;
                 anchor_visitor(Gq2ToL3kvgCompiler* c) : compiler(c) {}
                 void operator()(const irods::experimental::genquery2::condition& c) {
                     std::string col_name;
                     if (auto* col = std::get_if<irods::experimental::genquery2::column>(&c.lhs)) col_name = col->name;
                     auto it = COLUMN_NAME_MAP.find(col_name);
                     if (it != COLUMN_NAME_MAP.end()) {
                         std::string t = std::string(it->second.node_type);
                         int current_priority = 0;
                         std::string cur = std::string(compiler->get_entry_type());
                         if (cur == "DataObject") current_priority = 4;
                         else if (cur == "Collection") current_priority = 3;
                         else if (cur == "User" || cur == "Resource") current_priority = 2;
                         else if (cur == "Zone") current_priority = 1;
                         
                         int new_priority = 0;
                         if (t == "DataObject") new_priority = 4;
                         else if (t == "Collection") new_priority = 3;
                         else if (t == "User" || t == "Resource") new_priority = 2;
                         else if (t == "Zone") new_priority = 1;

                         if (new_priority > current_priority) {
                             compiler->set_entry_type(t);
                         }
                     }
                 }
                 void operator()(const irods::experimental::genquery2::logical_and& l) { for(const auto& c : l.condition) boost::apply_visitor(*this, c); }
                 void operator()(const irods::experimental::genquery2::logical_or& l) { for(const auto& c : l.condition) boost::apply_visitor(*this, c); }
                 void operator()(const irods::experimental::genquery2::logical_grouping& l) { for(const auto& c : l.conditions) boost::apply_visitor(*this, c); }
                 void operator()(const irods::experimental::genquery2::logical_not& l) { for(const auto& c : l.condition) boost::apply_visitor(*this, c); }
            };
            anchor_visitor av(this);
            for(const auto& w : ast.conditions) boost::apply_visitor(av, w);

            if (entry_node_type_.empty()) {
                for (const auto& p : ast.projections) {
                    struct peek_visitor : public boost::static_visitor<std::string_view> {
                        std::string_view operator()(const irods::experimental::genquery2::column& col) const {
                            auto it = COLUMN_NAME_MAP.find(col.name);
                            return it != COLUMN_NAME_MAP.end() ? it->second.node_type : "";
                        }
                        std::string_view operator()(const irods::experimental::genquery2::function& func) const {
                             for (const auto& arg : func.arguments) {
                                 if (auto* col = std::get_if<irods::experimental::genquery2::column>(&arg)) {
                                     auto it = COLUMN_NAME_MAP.find(col->name);
                                     if (it != COLUMN_NAME_MAP.end()) return it->second.node_type;
                                 }
                             }
                             return "";
                        }
                    } peek;
                    auto alias = boost::apply_visitor(peek, p);
                    if (!alias.empty()) {
                        entry_node_type_ = alias;
                        break;
                    }
                }
                if (entry_node_type_.empty()) {
                    entry_node_type_ = "DataObject";
                    rodsLog(LOG_NOTICE, "L3_COMPILER: Using default root alias: DataObject");
                }
            }
        }
        rodsLog(LOG_NOTICE, "L3_COMPILER: Final root alias: %s", entry_node_type_.c_str());

        json j;
        // 1. Collect all target node types from projections and filters
        json j_projs = json::array();
        projection_visitor pv(this, j_projs);
        for (const auto& p : ast.projections) {
            boost::apply_visitor(pv, p);
        }

        json j_filters = json::array();
        condition_visitor cv(this, j_filters);
        for(const auto& w : ast.conditions) {
            boost::apply_visitor(cv, w);
        }

        j["projections"] = j_projs;
        j["filters"] = j_filters;
        j["root_alias"] = entry_node_type_;

        // 2. Generate steps to all required target node types
        json j_steps = json::array();
        std::unordered_set<std::string> visited = { std::string(entry_node_type_) };
        for (const auto& target_view : target_node_types_) {
            std::string target(target_view);
            if (visited.count(target)) continue;
            rodsLog(LOG_NOTICE, "L3_COMPILER: Finding path from %s to %s", entry_node_type_.c_str(), target.c_str());
            auto path = find_path(entry_node_type_, target);
            
            std::string current_source = entry_node_type_;
            for (const auto& step : path) {
                if (visited.count(std::string(step.target_type))) {
                    current_source = step.target_type;
                    continue;
                }
                if (step.dir == Direction::Out) {
                    j_steps.push_back({{"type", "out"}, {"label", step.edge_label}, {"min_weight", 0.0}, {"target_alias", step.target_type}, {"source_alias", current_source}});
                } else {
                    j_steps.push_back({{"type", "in"}, {"label", step.edge_label}, {"target_alias", step.target_type}, {"source_alias", current_source}});
                }
                visited.insert(std::string(step.target_type));
                current_source = step.target_type;
            }
        }
        j["steps"] = j_steps;

        rodsLog(LOG_NOTICE, "L3_COMPILER: Compilation complete. Dumping JSON...");
        if (!ast.range.number_of_rows.empty()) j["limit"] = std::stoull(ast.range.number_of_rows);
        if (!ast.range.offset.empty()) j["offset"] = std::stoull(ast.range.offset);
        j["distinct"] = ast.distinct;

        return j.dump();
    }

    std::vector<Gq2ToL3kvgCompiler::PathStep> Gq2ToL3kvgCompiler::find_path(std::string_view source, std::string_view target) {
        auto it = ROUTING_TABLE.find({std::string(source), std::string(target)});
        if (it != ROUTING_TABLE.end()) return it->second;
        return {};
    }

} // namespace irods::catalog::compiler
