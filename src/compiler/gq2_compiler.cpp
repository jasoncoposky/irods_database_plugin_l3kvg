#include "irods/catalog/gq2_compiler.hpp"
#include "irods/rodsLog.h"
#include "irods/private/genquery2_sql.hpp"
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <string>
#include <cctype>
#include <stdexcept>
#include <type_traits>
#include <algorithm>
#include <nlohmann/json.hpp>
#include <boost/variant.hpp>
#include <boost/algorithm/string.hpp>
#include "irods/rodsGenQuery.h"

namespace irods::catalog::compiler {

    using json = nlohmann::json;
    namespace gq2 = irods::experimental::genquery2;
    using Direction = Gq2ToL3kvgCompiler::PathStep::Direction;

    template<class... Ts> struct overloaded : Ts... { using Ts::operator()...; };
    template<class... Ts> overloaded(Ts...) -> overloaded<Ts...>;

    const std::unordered_map<int, GraphMap> COLUMN_MAP = {
        {COL_D_DATA_ID,        {"DataObject", "id"}},
        {COL_D_COLL_ID,        {"Collection", "id"}},
        {COL_DATA_NAME,        {"DataObject", "n"}},
        {COL_DATA_SIZE,        {"Replica", "s"}},
        {COL_DATA_MODE,        {"DataObject", "mode"}},
        {COL_D_RESC_NAME,      {"Resource", "n"}},
        {COL_D_OWNER_NAME,     {"DataObject", "o"}},
        {COL_D_OWNER_ZONE,     {"DataObject", "z"}},
        {COL_D_CREATE_TIME,    {"DataObject", "ct"}},
        {COL_D_MODIFY_TIME,    {"Replica", "mt"}},
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
        {COL_USER_INFO,        {"User", "i"}},
        {COL_USER_COMMENT,     {"User", "c"}},
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
        {COL_D_ACCESS_TIME,    {"Replica", "at"}},

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
        {COL_COLL_ACCESS_USER_ID, {"User", "id"}},
        {COL_COLL_USER_NAME,      {"User", "n"}},
        {COL_COLL_USER_ZONE,      {"User", "z"}},
        {COL_DATA_USER_NAME,      {"User", "n"}},
        {COL_DATA_USER_ZONE,      {"User", "z"}},

        {COL_RULE_EXEC_ID,                  {"Rule", "id"}},
        {COL_RULE_EXEC_NAME,                {"Rule", "n"}},
        {COL_RULE_EXEC_REI_FILE_PATH,       {"Rule", "rei"}},
        {COL_RULE_EXEC_USER_NAME,           {"Rule", "u"}},
        {COL_RULE_EXEC_ADDRESS,             {"Rule", "addr"}},
        {COL_RULE_EXEC_TIME,                {"Rule", "e"}},
        {COL_RULE_EXEC_FREQUENCY,           {"Rule", "freq"}},
        {COL_RULE_EXEC_PRIORITY,            {"Rule", "p"}},
        {COL_RULE_EXEC_ESTIMATED_EXE_TIME,  {"Rule", "est"}},
        {COL_RULE_EXEC_NOTIFICATION_ADDR,   {"Rule", "notif"}},
        {COL_RULE_EXEC_LAST_EXE_TIME,       {"Rule", "last"}},
        {COL_RULE_EXEC_STATUS,              {"Rule", "status"}},
        {COL_RULE_EXEC_CONTEXT,             {"Rule", "ctx"}},
        {COL_RULE_EXEC_LOCK_HOST,           {"Rule", "lh"}},
        {COL_RULE_EXEC_LOCK_HOST_PID,       {"Rule", "lp"}},
        {COL_RULE_EXEC_LOCK_TIME,           {"Rule", "lt"}}
    };

    const std::unordered_map<std::string_view, GraphMap> COLUMN_NAME_MAP = {
        {"DATA_ID",           {"DataObject", "id"}},
        {"DATA_COLL_ID",      {"Collection", "id"}},
        {"DATA_NAME",         {"DataObject", "n"}},
        {"DATA_SIZE",         {"Replica", "s"}},
        {"DATA_MODE",         {"DataObject", "mode"}},
        {"DATA_VERSION",      {"DataObject", "v"}},
        {"DATA_TYPE_NAME",    {"DataObject", "t"}},
        {"DATA_OWNER_NAME",   {"DataObject", "o"}},
        {"DATA_OWNER_ZONE",   {"DataObject", "z"}},
        {"DATA_CREATE_TIME",  {"DataObject", "ct"}},
        {"DATA_MODIFY_TIME",  {"Replica", "mt"}},
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
        {"META_DATA_CREATE_TIME", {"Metadata", "ct"}},
        {"META_DATA_MODIFY_TIME", {"Metadata", "mt"}},
        {"META_COLL_ATTR_NAME",  {"Metadata", "a"}},
        {"META_COLL_ATTR_VALUE", {"Metadata", "v"}},
        {"META_COLL_ATTR_UNITS", {"Metadata", "u"}},
        {"META_COLL_ATTR_ID",    {"Metadata", "id"}},
        {"META_COLL_CREATE_TIME", {"Metadata", "ct"}},
        {"META_COLL_MODIFY_TIME", {"Metadata", "mt"}},
        {"META_RESC_ATTR_NAME",  {"Metadata", "a"}},
        {"META_RESC_ATTR_VALUE", {"Metadata", "v"}},
        {"META_RESC_ATTR_UNITS", {"Metadata", "u"}},
        {"META_RESC_ATTR_ID",    {"Metadata", "id"}},
        {"META_RESC_CREATE_TIME", {"Metadata", "ct"}},
        {"META_RESC_MODIFY_TIME", {"Metadata", "mt"}},
        {"META_USER_ATTR_NAME",  {"Metadata", "a"}},
        {"META_USER_ATTR_VALUE", {"Metadata", "v"}},
        {"META_USER_ATTR_UNITS", {"Metadata", "u"}},
        {"META_USER_ATTR_ID",    {"Metadata", "id"}},
        {"META_USER_CREATE_TIME", {"Metadata", "ct"}},
        {"META_USER_MODIFY_TIME", {"Metadata", "mt"}},

        {"META_DATA_ATTACHED_CREATE_TIME", {"Metadata", "ct"}},
        {"META_DATA_ATTACHED_MODIFY_TIME", {"Metadata", "mt"}},
        {"META_COLL_ATTACHED_CREATE_TIME", {"Metadata", "ct"}},
        {"META_COLL_ATTACHED_MODIFY_TIME", {"Metadata", "mt"}},
        {"META_RESC_ATTACHED_CREATE_TIME", {"Metadata", "ct"}},
        {"META_RESC_ATTACHED_MODIFY_TIME", {"Metadata", "mt"}},
        {"META_USER_ATTACHED_CREATE_TIME", {"Metadata", "ct"}},
        {"META_USER_ATTACHED_MODIFY_TIME", {"Metadata", "mt"}},

        {"USER_GROUP_ID",        {"Group", "id"}},
        {"USER_GROUP_NAME",      {"Group", "n"}},

        {"D_RESC_ID",      {"Replica", "rid"}},
        {"DATA_REPL_NUM",  {"Replica", "rn"}},
        {"DATA_PATH",      {"Replica", "p"}},
        {"DATA_REPL_STATUS",{"Replica", "st"}},
        {"D_RESC_HIER",    {"Replica", "rh"}},
        {"DATA_RESC_HIER", {"Replica", "rh"}},
        {"DATA_ACCESS_TIME", {"Replica", "at"}},

        {"DATA_ACCESS_NAME",      {"Access", "l"}},
        {"DATA_ACCESS_TYPE",      {"Access", "l"}},
        {"DATA_ACCESS_PERM_NAME", {"Access", "l"}},
        {"DATA_ACCESS_PERM_ID",   {"Access", "l"}},
        {"COLL_ACCESS_NAME",      {"CollAccess", "l"}},
        {"COLL_ACCESS_TYPE",      {"CollAccess", "l"}},
        {"COLL_ACCESS_PERM_NAME", {"CollAccess", "l"}},
        {"COLL_ACCESS_PERM_ID",   {"CollAccess", "l"}},
        {"DATA_TOKEN_NAMESPACE",  {"Access", "t"}},
        {"COLL_TOKEN_NAMESPACE",  {"CollAccess", "t"}},
        {"DATA_ACCESS_DATA_ID",   {"DataObject", "id"}},
        {"COLL_ACCESS_COLL_ID",   {"Collection", "id"}},
        {"COLL_COLL_ACCESS_COLL_ID", {"Collection", "id"}},
        {"DATA_ACCESS_USER_ID",   {"User", "id"}},
        {"DATA_ACCESS_USER_NAME", {"User", "n"}},
        {"DATA_ACCESS_USER_ZONE", {"User", "z"}},
        {"DATA_ACCESS_USER_TYPE", {"User", "t"}},
        {"COLL_ACCESS_USER_ID",   {"CollUser", "id"}},
        {"COLL_ACCESS_USER_NAME", {"CollUser", "n"}},
        {"COLL_ACCESS_USER_ZONE", {"CollUser", "z"}},
        {"COLL_ACCESS_USER_TYPE", {"CollUser", "t"}},
        {"COLL_USER_NAME",        {"CollUser", "n"}},
        {"COLL_USER_ZONE",        {"CollUser", "z"}},
        {"DATA_USER_NAME",        {"User", "n"}},
        {"DATA_USER_ZONE",        {"User", "z"}},

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
        {"TICKET_USER_ID",                {"Ticket", "uid"}},
        {"TICKET_OBJECT_ID",              {"Ticket", "oid"}},
        {"TICKET_OBJECT_TYPE",            {"Ticket", "ot"}},
        {"TICKET_DATA_COLL_NAME",         {"DataObject", "pn"}},
        {"TICKET_OWNER_ZONE",             {"User", "z"}},

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

        {"RULE_EXEC_ID",                  {"Rule", "id"}},
        {"RULE_EXEC_NAME",                {"Rule", "n"}},
        {"RULE_EXEC_REI_FILE_PATH",       {"Rule", "rei"}},
        {"RULE_EXEC_USER_NAME",           {"Rule", "u"}},
        {"RULE_EXEC_ADDRESS",             {"Rule", "addr"}},
        {"RULE_EXEC_TIME",                {"Rule", "e"}},
        {"RULE_EXEC_FREQUENCY",           {"Rule", "freq"}},
        {"RULE_EXEC_PRIORITY",            {"Rule", "p"}},
        {"RULE_EXEC_ESTIMATED_EXE_TIME",  {"Rule", "est"}},
        {"RULE_EXEC_NOTIFICATION_ADDR",   {"Rule", "notif"}},
        {"RULE_EXEC_LAST_EXE_TIME",       {"Rule", "last"}},
        {"RULE_EXEC_STATUS",              {"Rule", "status"}},
        {"RULE_EXEC_CONTEXT",             {"Rule", "ctx"}},
        {"RULE_EXEC_LOCK_HOST",           {"Rule", "lh"}},
        {"RULE_EXEC_LOCK_HOST_PID",       {"Rule", "lp"}},
        {"RULE_EXEC_LOCK_TIME",           {"Rule", "lt"}},

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
        {{"Collection", "CollAccess"}, {{Direction::In, "FOR_OBJECT", "CollAccess"}}},
        {{"Collection", "CollUser"},   {{Direction::In, "FOR_OBJECT", "CollAccess"}, {Direction::In, "HAS_ACCESS", "CollUser"}}},
        {{"Collection", "Access"},     {{Direction::Out, "CONTAINS", "DataObject"}, {Direction::In, "FOR_OBJECT", "Access"}}},
        {{"Collection", "User"},       {{Direction::Out, "CONTAINS", "DataObject"}, {Direction::In, "FOR_OBJECT", "Access"}, {Direction::In, "HAS_ACCESS", "User"}}},
        {{"Collection", "Group"},      {{Direction::In, "FOR_OBJECT", "CollAccess"}, {Direction::In, "HAS_ACCESS", "CollUser"}, {Direction::Out, "MEMBER_OF", "Group"}}},
        {{"DataObject", "CollAccess"}, {{Direction::In, "CONTAINS", "Collection"}, {Direction::In, "FOR_OBJECT", "CollAccess"}}},
        {{"DataObject", "CollUser"},   {{Direction::In, "CONTAINS", "Collection"}, {Direction::In, "FOR_OBJECT", "CollAccess"}, {Direction::In, "HAS_ACCESS", "CollUser"}}},
        {{"CollAccess", "CollUser"},   {{Direction::In, "HAS_ACCESS", "CollUser"}}},
        {{"CollUser", "CollAccess"},   {{Direction::Out, "HAS_ACCESS", "CollAccess"}}},
        {{"CollAccess", "Collection"}, {{Direction::Out, "FOR_OBJECT", "Collection"}}},
        {{"CollUser", "Collection"},   {{Direction::Out, "HAS_ACCESS", "CollAccess"}, {Direction::Out, "FOR_OBJECT", "Collection"}}},
        {{"Collection", "Zone"},       {{Direction::In, "HAS_ROOT_COLL", "Zone"}}},
        {{"Collection", "Replica"},    {{Direction::Out, "CONTAINS", "DataObject"}, {Direction::Out, "HAS_REPLICA", "Replica"}}},
        {{"User", "Replica"},          {{Direction::Out, "HAS_ACCESS", "Access"}, {Direction::Out, "FOR_OBJECT", "DataObject"}, {Direction::Out, "HAS_REPLICA", "Replica"}}},
        {{"Group", "Replica"},         {{Direction::In, "MEMBER_OF", "User"}, {Direction::Out, "HAS_ACCESS", "Access"}, {Direction::Out, "FOR_OBJECT", "DataObject"}, {Direction::Out, "HAS_REPLICA", "Replica"}}},
        {{"Zone", "Replica"},          {{Direction::Out, "HAS_ROOT_COLL", "Collection"}, {Direction::Out, "CONTAINS", "DataObject"}, {Direction::Out, "HAS_REPLICA", "Replica"}}},
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
        {{"User", "Metadata"},         {{Direction::Out, "ANNOTATED_WITH", "Metadata"}}},
        {{"Metadata", "DataObject"},   {{Direction::In, "ANNOTATED_WITH", "DataObject"}}},
        {{"Metadata", "Collection"},   {{Direction::In, "ANNOTATED_WITH", "Collection"}}},
        {{"Metadata", "User"},         {{Direction::In, "ANNOTATED_WITH", "User"}}},
        {{"Metadata", "Resource"},     {{Direction::In, "ANNOTATED_WITH", "Resource"}}},
        {{"Metadata", "Zone"},         {{Direction::In, "ANNOTATED_WITH", "Zone"}}},
        {{"Resource", "DataObject"},   {{Direction::In, "STAYING_AT", "Replica"}, {Direction::In, "HAS_REPLICA", "DataObject"}}},
        {{"Resource", "Replica"},      {{Direction::In, "STAYING_AT", "Replica"}}},
        {{"Resource", "Metadata"},     {{Direction::Out, "ANNOTATED_WITH", "Metadata"}}},
        {{"Resource", "Zone"},         {{Direction::In, "HAS_RESC", "Zone"}}},
        {{"Zone", "Collection"},       {{Direction::Out, "HAS_ROOT_COLL", "Collection"}}},
        {{"Zone", "User"},             {{Direction::Out, "HAS_USER", "User"}}},
        {{"Zone", "Resource"},         {{Direction::Out, "HAS_RESC", "Resource"}}},
        {{"Zone", "Metadata"},         {{Direction::Out, "ANNOTATED_WITH", "Metadata"}}},
        {{"Zone", "Rule"},             {{Direction::Out, "HAS_RULE", "Rule"}}},
        {{"Rule", "Zone"},             {{Direction::In, "HAS_RULE", "Zone"}}},
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
        std::pair<int, std::string> operator()(const irods::experimental::genquery2::condition_operator_not& c) const {
            if (const auto* lk = boost::get<irods::experimental::genquery2::condition_like>(&c.expression)) {
                return {7, lk->string_literal};
            }
            if (const auto* eq = boost::get<irods::experimental::genquery2::condition_equal>(&c.expression)) {
                return {1, eq->string_literal};
            }
            if (const auto* neq = boost::get<irods::experimental::genquery2::condition_not_equal>(&c.expression)) {
                return {0, neq->string_literal};
            }
            return {0, ""};
        }
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
            if (it == COLUMN_NAME_MAP.end()) {
                throw std::invalid_argument("Unknown column: " + col_name);
            }
            std::string node_type = std::string(it->second.node_type);
            std::string bson_key = std::string(it->second.bson_key);

            if (col_name == "USER_TYPE") {
                if (compiler->get_entry_type() == "Group") {
                    node_type = "Group";
                } else {
                    bool has_group = false;
                    for (const auto& t : compiler->get_target_types()) {
                        if (t == "Group") has_group = true;
                    }
                    if (has_group && compiler->get_entry_type() != "User") {
                        node_type = "Group";
                    }
                }
            }
            compiler->add_target_type(node_type);

            if (auto* in_expr = boost::get<irods::experimental::genquery2::condition_in>(&c.expression)) {
                if (in_expr->list_of_string_literals.empty()) {
                    return;
                }
                if (in_expr->list_of_string_literals.size() == 1) {
                    j_filters.push_back({{"alias", node_type}, {"key", bson_key}, {"op", 0}, {"value", in_expr->list_of_string_literals[0]}});
                } else {
                    json in_or = json::array();
                    for (size_t idx = 0; idx < in_expr->list_of_string_literals.size(); ++idx) {
                        json f = {{"alias", node_type}, {"key", bson_key}, {"op", 0}, {"value", in_expr->list_of_string_literals[idx]}};
                        if (idx > 0) {
                            f["prepended_op"] = "or";
                        }
                        in_or.push_back(f);
                    }
                    j_filters.push_back({{"group", "or"}, {"filters", in_or}, {"prepended_op", "and"}});
                }
                return;
            }

            if (auto* bet_expr = boost::get<irods::experimental::genquery2::condition_between>(&c.expression)) {
                json bet_and = json::array();
                bet_and.push_back({{"alias", node_type}, {"key", bson_key}, {"op", 3 /* >= */}, {"value", bet_expr->low}});
                bet_and.push_back({{"alias", node_type}, {"key", bson_key}, {"op", 5 /* <= */}, {"value", bet_expr->high}, {"prepended_op", "and"}});
                j_filters.push_back({{"group", "and"}, {"filters", bet_and}, {"prepended_op", "and"}});
                return;
            }

            pc_visitor pcv;
            auto pc = boost::apply_visitor(pcv, c.expression);
            if (node_type == "Access" && bson_key == "t" && pc.second == "access_type" && pc.first == 0) {
                json or_filters = json::array();
                or_filters.push_back({{"alias", node_type}, {"key", bson_key}, {"op", pc.first}, {"value", "access_type"}});
                or_filters.push_back({{"alias", node_type}, {"key", bson_key}, {"op", pc.first}, {"value", "access"}, {"prepended_op", "or"}});
                j_filters.push_back({{"group", "or"}, {"filters", or_filters}, {"prepended_op", "and"}});
                return;
            }
            j_filters.push_back({{"alias", node_type}, {"key", bson_key}, {"op", pc.first}, {"value", pc.second}});
        }

        void operator()(const irods::experimental::genquery2::logical_and& l) const { for(const auto& c : l.condition) boost::apply_visitor(*this, c); }
        void operator()(const irods::experimental::genquery2::logical_or& l) const {
            json branch_filters = json::array();
            for (const auto& c : l.condition) {
                condition_visitor sub_vis(compiler, branch_filters);
                boost::apply_visitor(sub_vis, c);
            }
            if (branch_filters.empty()) return;
            if (branch_filters.size() == 1) {
                j_filters.push_back(branch_filters[0]);
            } else {
                for (size_t idx = 1; idx < branch_filters.size(); ++idx) {
                    branch_filters[idx]["prepended_op"] = "or";
                }
                j_filters.push_back({{"group", "or"}, {"filters", branch_filters}, {"prepended_op", "and"}});
            }
        }
        void operator()(const irods::experimental::genquery2::logical_grouping& l) const {
            json grp_filters = json::array();
            for(const auto& c : l.conditions) {
                condition_visitor sub_vis(compiler, grp_filters);
                boost::apply_visitor(sub_vis, c);
            }
            if (!grp_filters.empty()) {
                j_filters.push_back({{"group", "and"}, {"filters", grp_filters}, {"prepended_op", "and"}});
            }
        }
        void operator()(const irods::experimental::genquery2::logical_not& l) const { for(const auto& c : l.condition) boost::apply_visitor(*this, c); }
    };

    struct projection_visitor : public boost::static_visitor<void> {
        Gq2ToL3kvgCompiler* compiler;
        json& j_projs;
        mutable int col_idx = 0;
        projection_visitor(Gq2ToL3kvgCompiler* c, json& jp) : compiler(c), j_projs(jp) {}

        void operator()(const irods::experimental::genquery2::column& col) const {
            if (col.name.rfind("DATA_ACCESS_", 0) == 0 && col.name != "DATA_ACCESS_TIME" && col.name != "DATA_ACCESS_DATA_ID") {
                compiler->add_target_type("DataObject");
                compiler->add_target_type("Access");
            } else if (col.name.rfind("COLL_ACCESS_", 0) == 0 && col.name != "COLL_ACCESS_COLL_ID" && col.name != "COLL_COLL_ACCESS_COLL_ID") {
                compiler->add_target_type("Collection");
                compiler->add_target_type("CollAccess");
            }
            auto it = COLUMN_NAME_MAP.find(col.name);
            if (it != COLUMN_NAME_MAP.end()) {
                compiler->add_target_type(it->second.node_type);
                j_projs.push_back({{"alias", it->second.node_type}, {"property", it->second.bson_key}, {"agg", 0}, {"as", "idx_" + std::to_string(col_idx++)}});
            } else {
                throw std::invalid_argument("Unknown column: " + col.name);
            }
        }

        void operator()(const irods::experimental::genquery2::function& func) const {
            std::string upper_name = boost::algorithm::to_upper_copy(func.name);
            int agg = 0;
            if (upper_name == "COUNT") agg = 1;
            else if (upper_name == "SUM") agg = 2;
            else if (upper_name == "AVG") agg = 3;
            else if (upper_name == "MIN") agg = 4;
            else if (upper_name == "MAX") agg = 5;

            for (const auto& arg : func.arguments) {
                if (auto* col = std::get_if<irods::experimental::genquery2::column>(&arg)) {
                    if (col->name.rfind("DATA_ACCESS_", 0) == 0 && col->name != "DATA_ACCESS_TIME" && col->name != "DATA_ACCESS_DATA_ID") {
                        compiler->add_target_type("DataObject");
                        compiler->add_target_type("Access");
                    } else if (col->name.rfind("COLL_ACCESS_", 0) == 0 && col->name != "COLL_ACCESS_COLL_ID" && col->name != "COLL_COLL_ACCESS_COLL_ID") {
                        compiler->add_target_type("Collection");
                        compiler->add_target_type("CollAccess");
                    }
                    auto it = COLUMN_NAME_MAP.find(col->name);
                    if (it != COLUMN_NAME_MAP.end()) {
                        compiler->add_target_type(it->second.node_type);
                        j_projs.push_back({{"alias", it->second.node_type}, {"property", it->second.bson_key}, {"agg", agg}, {"distinct", func.distinct}, {"as", "idx_" + std::to_string(col_idx++)}});
                    } else {
                        throw std::invalid_argument("Unknown column: " + col->name);
                    }
                }
            }
        }
    };
 
    namespace {
        std::string normalize_entity_type(std::string_view raw);
    }

    std::string Gq2ToL3kvgCompiler::compile(const irods::experimental::genquery2::select& ast, std::string_view override_root_alias, const irods::experimental::genquery2::options* opts) {
        rodsLog(LOG_NOTICE, "L3_COMPILER: Entering compile()");

        if (!override_root_alias.empty()) {
            entry_node_type_ = override_root_alias;
            rodsLog(LOG_NOTICE, "L3_COMPILER: Using override root alias: %s", entry_node_type_.c_str());
        } else if (!ast.from_entity.empty()) {
            entry_node_type_ = normalize_entity_type(ast.from_entity);
            rodsLog(LOG_NOTICE, "L3_COMPILER: Using ast.from_entity root alias: %s", entry_node_type_.c_str());
        } else {
            struct anchor_visitor : public boost::static_visitor<void> {
                 Gq2ToL3kvgCompiler* compiler;
                 anchor_visitor(Gq2ToL3kvgCompiler* c) : compiler(c) {}
                 void operator()(const irods::experimental::genquery2::condition& c) {
                     std::string col_name;
                     if (auto* col = std::get_if<irods::experimental::genquery2::column>(&c.lhs)) col_name = col->name;
                     else if (auto* func = std::get_if<irods::experimental::genquery2::function>(&c.lhs)) col_name = func->name;
                     auto it = COLUMN_NAME_MAP.find(col_name);
                     if (it != COLUMN_NAME_MAP.end()) {
                         std::string t = std::string(it->second.node_type);
                         if (col_name.rfind("DATA_ACCESS_", 0) == 0 || col_name.rfind("DATA_", 0) == 0) t = "DataObject";
                         else if (col_name.rfind("COLL_ACCESS_", 0) == 0 || col_name.rfind("COLL_", 0) == 0) t = "Collection";
                         else if (t == "CollUser" || t == "CollAccess") t = "Collection";
                         else if (t == "Access" || t == "Replica") t = "DataObject";

                         int current_priority = 0;
                         std::string cur = std::string(compiler->get_entry_type());
                         if (cur == "DataObject") current_priority = 5;
                         else if (cur == "Collection") current_priority = 4;
                         else if (cur == "Group") current_priority = 3;
                         else if (cur == "User" || cur == "Resource") current_priority = 2;
                         else if (cur == "Zone") current_priority = 1;
                         
                         int new_priority = 0;
                         if (t == "DataObject") new_priority = 5;
                         else if (t == "Collection") new_priority = 4;
                         else if (t == "Group") new_priority = 3;
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
                    struct peek_visitor : public boost::static_visitor<std::string> {
                        std::string operator()(const irods::experimental::genquery2::column& col) const {
                            if (col.name.rfind("DATA_ACCESS_", 0) == 0 || col.name.rfind("DATA_", 0) == 0) return "DataObject";
                            if (col.name.rfind("COLL_ACCESS_", 0) == 0 || col.name.rfind("COLL_", 0) == 0) return "Collection";
                            auto it = COLUMN_NAME_MAP.find(col.name);
                            if (it != COLUMN_NAME_MAP.end()) {
                                std::string t(it->second.node_type);
                                if (t == "CollUser" || t == "CollAccess") return "Collection";
                                if (t == "Access" || t == "Replica") return "DataObject";
                                return t;
                            }
                            return "";
                        }
                        std::string operator()(const irods::experimental::genquery2::function& func) const {
                             for (const auto& arg : func.arguments) {
                                 if (auto* col = std::get_if<irods::experimental::genquery2::column>(&arg)) {
                                     if (col->name.rfind("DATA_ACCESS_", 0) == 0 || col->name.rfind("DATA_", 0) == 0) return "DataObject";
                                     if (col->name.rfind("COLL_ACCESS_", 0) == 0 || col->name.rfind("COLL_", 0) == 0) return "Collection";
                                     auto it = COLUMN_NAME_MAP.find(col->name);
                                     if (it != COLUMN_NAME_MAP.end()) {
                                         std::string t(it->second.node_type);
                                         if (t == "CollUser" || t == "CollAccess") return "Collection";
                                         if (t == "Access" || t == "Replica") return "DataObject";
                                         return t;
                                     }
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

        // 2. Collect targets from group_by
        if (!ast.group_by.expressions.empty()) {
            json j_groups = json::array();
            for (const auto& expr : ast.group_by.expressions) {
                if (const auto* col = std::get_if<irods::experimental::genquery2::column>(&expr)) {
                    auto it = COLUMN_NAME_MAP.find(col->name);
                    if (it == COLUMN_NAME_MAP.end()) {
                        throw std::invalid_argument("Unknown column: " + col->name);
                    }
                    add_target_type(it->second.node_type);
                    j_groups.push_back({{"alias", it->second.node_type}, {"property", it->second.bson_key}});
                } else if (const auto* func = std::get_if<irods::experimental::genquery2::function>(&expr)) {
                    std::string fn_name = boost::algorithm::to_upper_copy(func->name);
                    std::string col_alias;
                    std::string col_prop;
                    std::vector<std::string> fn_args;
                    for (const auto& arg : func->arguments) {
                        if (const auto* c = std::get_if<irods::experimental::genquery2::column>(&arg)) {
                            auto it = COLUMN_NAME_MAP.find(c->name);
                            if (it == COLUMN_NAME_MAP.end()) {
                                throw std::invalid_argument("Unknown column: " + c->name);
                            }
                            add_target_type(it->second.node_type);
                            col_alias = it->second.node_type;
                            col_prop = it->second.bson_key;
                        } else if (const auto* s = std::get_if<std::string>(&arg)) {
                            fn_args.push_back(*s);
                        }
                    }
                    j_groups.push_back({{"alias", col_alias}, {"property", col_prop}, {"func_name", fn_name}, {"func_args", fn_args}});
                }
            }
            j["groups"] = j_groups;
        }

        // 3. Collect targets from order_by
        if (!ast.order_by.sort_expressions.empty()) {
            json j_sorts = json::array();
            for (const auto& se : ast.order_by.sort_expressions) {
                if (const auto* col = std::get_if<irods::experimental::genquery2::column>(&se.expr)) {
                    auto it = COLUMN_NAME_MAP.find(col->name);
                    if (it == COLUMN_NAME_MAP.end()) {
                        throw std::invalid_argument("Unknown column: " + col->name);
                    }
                    add_target_type(it->second.node_type);
                    j_sorts.push_back({{"alias", it->second.node_type}, {"property", it->second.bson_key}, {"ascending", se.ascending_order}});
                } else if (const auto* func = std::get_if<irods::experimental::genquery2::function>(&se.expr)) {
                    for (const auto& arg : func->arguments) {
                        if (const auto* c = std::get_if<irods::experimental::genquery2::column>(&arg)) {
                            auto it = COLUMN_NAME_MAP.find(c->name);
                            if (it == COLUMN_NAME_MAP.end()) {
                                throw std::invalid_argument("Unknown column: " + c->name);
                            }
                            add_target_type(it->second.node_type);
                            j_sorts.push_back({{"alias", it->second.node_type}, {"property", it->second.bson_key}, {"ascending", se.ascending_order}});
                            break;
                        }
                    }
                }
            }
            j["sorts"] = j_sorts;
        }

        // 4. If DataObject is root or targeted, ensure Replica layer semantics
        bool has_data_object = (entry_node_type_ == "DataObject");
        bool has_collection = (entry_node_type_ == "Collection");
        for (const auto& t : target_node_types_) {
            if (t == "DataObject") {
                has_data_object = true;
            } else if (t == "Collection") {
                has_collection = true;
            }
        }
        if (has_data_object) {
            add_target_type("Replica");
        }

        // 5. Security & Permission filtering for unprivileged users
        if (opts && !opts->admin_mode && !opts->user_name.empty()) {
            auto add_access_filter = [&](const std::string& alias) {
                json access_filter = {
                    {"alias", alias},
                    {"key", "_access_user"},
                    {"op", 0},
                    {"value", std::string(opts->user_name)},
                    {"prepended_op", "and"}
                };
                if (j_filters.empty()) {
                    j_filters.push_back(access_filter);
                } else {
                    json wrapped = json::array();
                    wrapped.push_back(access_filter);
                    wrapped.push_back({{"group", "and"}, {"filters", j_filters}, {"prepended_op", "and"}});
                    j_filters = wrapped;
                }
            };

            if (has_data_object) {
                add_access_filter("DataObject");
            }
            if (has_collection) {
                add_access_filter("Collection");
            }
        }

        j["projections"] = j_projs;
        j["filters"] = j_filters;
        j["root_alias"] = entry_node_type_;

        // 6. Generate steps to all required target node types
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

    namespace {
        std::string normalize_entity_type(std::string_view raw) {
            if (raw.empty()) {
                return "";
            }
            std::string s;
            s.reserve(raw.size());
            for (char c : raw) {
                s.push_back(static_cast<char>(std::toupper(static_cast<unsigned char>(c))));
            }

            if (s == "DATA_OBJECT" || s == "DATAOBJECT" || s == "DATA_NAME" || s == "DATA" || s == "DATAOBJECTS") {
                return "DataObject";
            }
            if (s == "COLLECTION" || s == "COLL" || s == "COLL_NAME" || s == "COLLECTIONS") {
                return "Collection";
            }
            if (s == "USER" || s == "USER_NAME" || s == "USERS") {
                return "User";
            }
            if (s == "RESOURCE" || s == "RESC" || s == "RESC_NAME" || s == "RESOURCES") {
                return "Resource";
            }
            if (s == "ZONE" || s == "ZONE_NAME" || s == "ZONES") {
                return "Zone";
            }
            if (s == "METADATA" || s == "AVU" || s == "META") {
                return "Metadata";
            }
            if (s == "REPLICA" || s == "REPL" || s == "REPLICAS") {
                return "Replica";
            }
            if (s == "GROUP" || s == "GROUPS" || s == "USER_GROUP" || s == "USER_GROUPS" || s == "R_USER_GROUP") {
                return "Group";
            }
            if (s == "ACCESS") {
                return "Access";
            }
            if (s == "TICKET" || s == "TICKETS") {
                return "Ticket";
            }
            if (s == "MSRVC") {
                return "MSRVC";
            }
            if (s == "AUDIT") {
                return "Audit";
            }
            if (s == "SERVERLOAD" || s == "SERVER_LOAD") {
                return "ServerLoad";
            }
            if (s == "RULE" || s == "RULES" || s == "RULE_EXEC" || s == "R_RULE_EXEC") {
                return "Rule";
            }
            if (s == "DVM") {
                return "DVM";
            }
            if (s == "FNM") {
                return "FNM";
            }
            if (s == "QUOTA") {
                return "Quota";
            }

            return std::string(raw);
        }

        int entity_priority(std::string_view et) {
            if (et == "DataObject") return 5;
            if (et == "Collection") return 4;
            if (et == "Group") return 3;
            if (et == "User" || et == "Resource") return 2;
            if (et == "Zone") return 1;
            return 0;
        }

        const GraphMap* find_column_mapping(std::string_view col) {
            auto it = COLUMN_NAME_MAP.find(col);
            if (it != COLUMN_NAME_MAP.end()) {
                return &it->second;
            }
            std::string upper;
            upper.reserve(col.size());
            for (char c : col) upper.push_back(static_cast<char>(std::toupper(static_cast<unsigned char>(c))));
            auto it_upper = COLUMN_NAME_MAP.find(upper);
            if (it_upper != COLUMN_NAME_MAP.end()) {
                return &it_upper->second;
            }
            return nullptr;
        }

        void map_assignment(const std::string& col, const std::string& val, const std::string& entity_type, std::unordered_map<std::string, std::string>& properties) {
            std::string col_upper;
            col_upper.reserve(col.size());
            for (char c : col) col_upper.push_back(static_cast<char>(std::toupper(static_cast<unsigned char>(c))));

            if (entity_type == "DataObject" && (col_upper == "COLL_NAME" || col_upper == "PARENT_COLL" || col == "parent_coll")) {
                properties["parent_coll"] = val;
                return;
            }
            if (entity_type == "DataObject" && (col_upper == "COLL_ID" || col_upper == "DATA_COLL_ID" || col == "coll_id")) {
                properties["coll_id"] = val;
                return;
            }

            const auto* gm = find_column_mapping(col);
            if (gm) {
                properties[std::string(gm->bson_key)] = val;
            } else {
                properties[col] = val;
            }
        }

        struct entity_infer_visitor : public boost::static_visitor<void> {
            std::string& entity_type;
            int& best_priority;
            entity_infer_visitor(std::string& et, int& bp) : entity_type(et), best_priority(bp) {}

            void operator()(const irods::experimental::genquery2::condition& c) const {
                std::string col_name;
                if (auto* col = std::get_if<irods::experimental::genquery2::column>(&c.lhs)) {
                    col_name = col->name;
                } else if (auto* func = std::get_if<irods::experimental::genquery2::function>(&c.lhs)) {
                    for (const auto& arg : func->arguments) {
                        if (auto* arg_col = std::get_if<irods::experimental::genquery2::column>(&arg)) {
                            col_name = arg_col->name;
                            break;
                        }
                    }
                    if (col_name.empty()) col_name = func->name;
                }
                if (col_name.empty()) return;
                const auto* gm = find_column_mapping(col_name);
                if (gm) {
                    int p = entity_priority(gm->node_type);
                    if (p > best_priority) {
                        best_priority = p;
                        entity_type = std::string(gm->node_type);
                    }
                }
            }

            void operator()(const irods::experimental::genquery2::logical_and& l) const {
                for (const auto& c : l.condition) boost::apply_visitor(*this, c);
            }
            void operator()(const irods::experimental::genquery2::logical_or& l) const {
                for (const auto& c : l.condition) boost::apply_visitor(*this, c);
            }
            void operator()(const irods::experimental::genquery2::logical_grouping& l) const {
                for (const auto& c : l.conditions) boost::apply_visitor(*this, c);
            }
            void operator()(const irods::experimental::genquery2::logical_not& l) const {
                for (const auto& c : l.condition) boost::apply_visitor(*this, c);
            }
        };

        struct dml_condition_visitor : public boost::static_visitor<void> {
            const std::string& entity_type;
            std::vector<DmlCondition>& conditions;

            dml_condition_visitor(const std::string& et, std::vector<DmlCondition>& conds)
                : entity_type(et), conditions(conds) {}

            void operator()(const irods::experimental::genquery2::condition& c) const {
                std::string col_name;
                if (auto* col = std::get_if<irods::experimental::genquery2::column>(&c.lhs)) {
                    col_name = col->name;
                } else if (auto* func = std::get_if<irods::experimental::genquery2::function>(&c.lhs)) {
                    for (const auto& arg : func->arguments) {
                        if (auto* arg_col = std::get_if<irods::experimental::genquery2::column>(&arg)) {
                            col_name = arg_col->name;
                            break;
                        }
                    }
                    if (col_name.empty()) col_name = func->name;
                }
                if (col_name.empty()) return;

                const auto* gm = find_column_mapping(col_name);

                std::string col_upper;
                col_upper.reserve(col_name.size());
                for (char ch : col_name) col_upper.push_back(static_cast<char>(std::toupper(static_cast<unsigned char>(ch))));

                std::string key;
                if (entity_type == "DataObject" && (col_upper == "COLL_NAME" || col_upper == "PARENT_COLL" || col_name == "parent_coll")) {
                    key = "parent_coll";
                } else if (gm) {
                    key = std::string(gm->bson_key);
                } else {
                    key = col_name;
                }

                pc_visitor pcv;
                auto pc = boost::apply_visitor(pcv, c.expression);
                conditions.push_back({std::move(key), pc.first, std::move(pc.second)});
            }

            void operator()(const irods::experimental::genquery2::logical_and& l) const {
                for (const auto& c : l.condition) boost::apply_visitor(*this, c);
            }
            void operator()(const irods::experimental::genquery2::logical_or&) const {
                throw std::invalid_argument("DML WHERE clause does not support logical OR");
            }
            void operator()(const irods::experimental::genquery2::logical_grouping& l) const {
                for (const auto& c : l.conditions) boost::apply_visitor(*this, c);
            }
            void operator()(const irods::experimental::genquery2::logical_not&) const {
                throw std::invalid_argument("DML WHERE clause does not support logical NOT");
            }
        };
    } // anonymous namespace

    DmlPlan Gq2ToL3kvgCompiler::compile(const irods::experimental::genquery2::insert& ast) const {
        DmlPlan plan;
        plan.action = DmlAction::Insert;
        plan.entity_type = normalize_entity_type(ast.target_entity);

        if (plan.entity_type.empty()) {
            int best_prio = 0;
            for (const auto& [col, val] : ast.assignments) {
                const auto* gm = find_column_mapping(col);
                if (gm) {
                    int p = entity_priority(gm->node_type);
                    if (p > best_prio) {
                        best_prio = p;
                        plan.entity_type = std::string(gm->node_type);
                    }
                }
            }
        }

        for (const auto& [col, val] : ast.assignments) {
            map_assignment(col, val, plan.entity_type, plan.properties);
        }

        return plan;
    }

    DmlPlan Gq2ToL3kvgCompiler::compile(const irods::experimental::genquery2::update& ast) const {
        DmlPlan plan;
        plan.action = DmlAction::Update;
        plan.entity_type = normalize_entity_type(ast.target_entity);

        int best_prio = 0;
        if (plan.entity_type.empty()) {
            for (const auto& [col, val] : ast.assignments) {
                const auto* gm = find_column_mapping(col);
                if (gm) {
                    int p = entity_priority(gm->node_type);
                    if (p > best_prio) {
                        best_prio = p;
                        plan.entity_type = std::string(gm->node_type);
                    }
                }
            }
        }

        if (plan.entity_type.empty() || best_prio < 4) {
            entity_infer_visitor eiv(plan.entity_type, best_prio);
            for (const auto& w : ast.where_conditions) {
                boost::apply_visitor(eiv, w);
            }
        }

        for (const auto& [col, val] : ast.assignments) {
            map_assignment(col, val, plan.entity_type, plan.properties);
        }

        dml_condition_visitor cv(plan.entity_type, plan.conditions);
        for (const auto& w : ast.where_conditions) {
            boost::apply_visitor(cv, w);
        }

        return plan;
    }

    DmlPlan Gq2ToL3kvgCompiler::compile(const irods::experimental::genquery2::remove& ast) const {
        DmlPlan plan;
        plan.action = DmlAction::Remove;
        plan.entity_type = normalize_entity_type(ast.target_entity);

        if (plan.entity_type.empty()) {
            int best_prio = 0;
            entity_infer_visitor eiv(plan.entity_type, best_prio);
            for (const auto& w : ast.where_conditions) {
                boost::apply_visitor(eiv, w);
            }
        }

        dml_condition_visitor cv(plan.entity_type, plan.conditions);
        for (const auto& w : ast.where_conditions) {
            boost::apply_visitor(cv, w);
        }

        return plan;
    }

    DmlPlan Gq2ToL3kvgCompiler::compile(const irods::experimental::genquery2::statement& ast) const {
        return std::visit([this](const auto& s) -> DmlPlan {
            using T = std::decay_t<decltype(s)>;
            if constexpr (std::is_same_v<T, irods::experimental::genquery2::insert>) {
                return this->compile(s);
            } else if constexpr (std::is_same_v<T, irods::experimental::genquery2::update>) {
                return this->compile(s);
            } else if constexpr (std::is_same_v<T, irods::experimental::genquery2::remove>) {
                return this->compile(s);
            } else {
                throw std::invalid_argument("Cannot compile select statement to DmlPlan");
            }
        }, ast);
    }

} // namespace irods::catalog::compiler
