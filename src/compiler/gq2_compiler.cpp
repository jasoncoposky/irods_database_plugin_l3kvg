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
#include "buffer.hpp"
#include <boost/variant.hpp>
#include <boost/algorithm/string.hpp>
#include "irods/rodsGenQuery.h"

namespace irods::catalog::compiler {

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

    static const GraphMap* find_column_mapping(std::string_view col) {
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
        lite3cpp::Buffer& buf;
        size_t arr_ofs;
        condition_visitor(Gq2ToL3kvgCompiler* c, lite3cpp::Buffer& b, size_t a) : compiler(c), buf(b), arr_ofs(a) {}

        static void append_filter(lite3cpp::Buffer& b, size_t a_ofs, std::string_view alias, std::string_view key, int64_t op, std::string_view val, std::string_view prepended_op = "") {
            size_t f_ofs = b.arr_append_obj(a_ofs);
            b.set_str(f_ofs, "alias", alias);
            b.set_str(f_ofs, "key", key);
            b.set_i64(f_ofs, "op", op);
            b.set_str(f_ofs, "value", val);
            if (!prepended_op.empty()) {
                b.set_str(f_ofs, "prepended_op", prepended_op);
            }
        }

        static void copy_filter_element(const lite3cpp::Buffer& src, size_t src_arr_ofs, uint32_t src_idx,
                                        lite3cpp::Buffer& dst, size_t dst_arr_ofs,
                                        std::string_view override_prepended_op = "") {
            if (src.arr_get_type(src_arr_ofs, src_idx) != lite3cpp::Type::Object) return;
            size_t s_ofs = src.arr_get_obj(src_arr_ofs, src_idx);
            size_t d_ofs = dst.arr_append_obj(dst_arr_ofs);
            if (src.get_type(s_ofs, "group") != lite3cpp::Type::Invalid) {
                dst.set_str(d_ofs, "group", src.get_str(s_ofs, "group"));
                if (!override_prepended_op.empty()) {
                    dst.set_str(d_ofs, "prepended_op", override_prepended_op);
                } else if (src.get_type(s_ofs, "prepended_op") == lite3cpp::Type::String) {
                    dst.set_str(d_ofs, "prepended_op", src.get_str(s_ofs, "prepended_op"));
                }
                if (src.get_type(s_ofs, "filters") == lite3cpp::Type::Array) {
                    size_t src_sub_arr = src.get_arr(s_ofs, "filters");
                    size_t dst_sub_arr = dst.set_arr(d_ofs, "filters");
                    lite3cpp::NodeView sub_nv(reinterpret_cast<const lite3cpp::PackedNodeLayout*>(src.data() + src_sub_arr));
                    for (uint32_t i = 0; i < sub_nv.size(); ++i) {
                        copy_filter_element(src, src_sub_arr, i, dst, dst_sub_arr);
                    }
                }
            } else {
                if (src.get_type(s_ofs, "alias") == lite3cpp::Type::String) dst.set_str(d_ofs, "alias", src.get_str(s_ofs, "alias"));
                if (src.get_type(s_ofs, "key") == lite3cpp::Type::String) dst.set_str(d_ofs, "key", src.get_str(s_ofs, "key"));
                if (src.get_type(s_ofs, "op") == lite3cpp::Type::Int64) dst.set_i64(d_ofs, "op", src.get_i64(s_ofs, "op"));
                if (src.get_type(s_ofs, "value") == lite3cpp::Type::String) dst.set_str(d_ofs, "value", src.get_str(s_ofs, "value"));
                if (!override_prepended_op.empty()) {
                    dst.set_str(d_ofs, "prepended_op", override_prepended_op);
                } else if (src.get_type(s_ofs, "prepended_op") == lite3cpp::Type::String) {
                    dst.set_str(d_ofs, "prepended_op", src.get_str(s_ofs, "prepended_op"));
                }
            }
        }

        void operator()(const irods::experimental::genquery2::condition& c) const {
            std::string col_name;
            if (auto* col = std::get_if<irods::experimental::genquery2::column>(&c.lhs)) col_name = col->name;
            else if (auto* func = std::get_if<irods::experimental::genquery2::function>(&c.lhs)) col_name = func->name;
            const auto* gm = find_column_mapping(col_name);
            if (!gm) {
                throw std::invalid_argument("Unknown column: " + col_name);
            }
            std::string node_type = std::string(gm->node_type);
            std::string bson_key = std::string(gm->bson_key);

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
                    append_filter(buf, arr_ofs, node_type, bson_key, 0, in_expr->list_of_string_literals[0]);
                } else {
                    size_t grp_ofs = buf.arr_append_obj(arr_ofs);
                    buf.set_str(grp_ofs, "group", "or");
                    buf.set_str(grp_ofs, "prepended_op", "and");
                    size_t in_or = buf.set_arr(grp_ofs, "filters");
                    for (size_t idx = 0; idx < in_expr->list_of_string_literals.size(); ++idx) {
                        append_filter(buf, in_or, node_type, bson_key, 0, in_expr->list_of_string_literals[idx], idx > 0 ? "or" : "");
                    }
                }
                return;
            }

            if (auto* bet_expr = boost::get<irods::experimental::genquery2::condition_between>(&c.expression)) {
                size_t grp_ofs = buf.arr_append_obj(arr_ofs);
                buf.set_str(grp_ofs, "group", "and");
                buf.set_str(grp_ofs, "prepended_op", "and");
                size_t bet_and = buf.set_arr(grp_ofs, "filters");
                append_filter(buf, bet_and, node_type, bson_key, 3 /* >= */, bet_expr->low);
                append_filter(buf, bet_and, node_type, bson_key, 5 /* <= */, bet_expr->high, "and");
                return;
            }

            pc_visitor pcv;
            auto pc = boost::apply_visitor(pcv, c.expression);
            if (node_type == "Access" && bson_key == "t" && pc.second == "access_type" && pc.first == 0) {
                size_t grp_ofs = buf.arr_append_obj(arr_ofs);
                buf.set_str(grp_ofs, "group", "or");
                buf.set_str(grp_ofs, "prepended_op", "and");
                size_t or_filters = buf.set_arr(grp_ofs, "filters");
                append_filter(buf, or_filters, node_type, bson_key, pc.first, "access_type");
                append_filter(buf, or_filters, node_type, bson_key, pc.first, "access", "or");
                return;
            }
            append_filter(buf, arr_ofs, node_type, bson_key, pc.first, pc.second);
        }

        void operator()(const irods::experimental::genquery2::logical_and& l) const { for(const auto& c : l.condition) boost::apply_visitor(*this, c); }
        void operator()(const irods::experimental::genquery2::logical_or& l) const {
            lite3cpp::Buffer branch_buf;
            branch_buf.init_array();
            condition_visitor sub_vis(compiler, branch_buf, 0);
            for (const auto& c : l.condition) {
                boost::apply_visitor(sub_vis, c);
            }
            if (branch_buf.size() < sizeof(lite3cpp::PackedNodeLayout)) return;
            lite3cpp::NodeView bnv(reinterpret_cast<const lite3cpp::PackedNodeLayout*>(branch_buf.data()));
            if (bnv.size() == 0) return;
            if (bnv.size() == 1) {
                copy_filter_element(branch_buf, 0, 0, buf, arr_ofs);
            } else {
                size_t grp_ofs = buf.arr_append_obj(arr_ofs);
                buf.set_str(grp_ofs, "group", "or");
                buf.set_str(grp_ofs, "prepended_op", "and");
                size_t or_arr = buf.set_arr(grp_ofs, "filters");
                for (uint32_t idx = 0; idx < bnv.size(); ++idx) {
                    copy_filter_element(branch_buf, 0, idx, buf, or_arr, idx > 0 ? "or" : "");
                }
            }
        }
        void operator()(const irods::experimental::genquery2::logical_grouping& l) const {
            lite3cpp::Buffer grp_buf;
            grp_buf.init_array();
            condition_visitor sub_vis(compiler, grp_buf, 0);
            for(const auto& c : l.conditions) {
                boost::apply_visitor(sub_vis, c);
            }
            if (grp_buf.size() < sizeof(lite3cpp::PackedNodeLayout)) return;
            lite3cpp::NodeView gnv(reinterpret_cast<const lite3cpp::PackedNodeLayout*>(grp_buf.data()));
            if (gnv.size() > 0) {
                size_t grp_ofs = buf.arr_append_obj(arr_ofs);
                buf.set_str(grp_ofs, "group", "and");
                buf.set_str(grp_ofs, "prepended_op", "and");
                size_t sub_arr = buf.set_arr(grp_ofs, "filters");
                for (uint32_t i = 0; i < gnv.size(); ++i) {
                    copy_filter_element(grp_buf, 0, i, buf, sub_arr);
                }
            }
        }
        void operator()(const irods::experimental::genquery2::logical_not& l) const { for(const auto& c : l.condition) boost::apply_visitor(*this, c); }
    };

    struct projection_visitor : public boost::static_visitor<void> {
        Gq2ToL3kvgCompiler* compiler;
        lite3cpp::Buffer& buf;
        size_t projs_arr_ofs;
        mutable int col_idx = 0;
        projection_visitor(Gq2ToL3kvgCompiler* c, lite3cpp::Buffer& b, size_t p) : compiler(c), buf(b), projs_arr_ofs(p) {}

        void operator()(const irods::experimental::genquery2::column& col) const {
            if (col.name.rfind("DATA_ACCESS_", 0) == 0 && col.name != "DATA_ACCESS_TIME" && col.name != "DATA_ACCESS_DATA_ID") {
                compiler->add_target_type("DataObject");
                compiler->add_target_type("Access");
            } else if (col.name.rfind("COLL_ACCESS_", 0) == 0 && col.name != "COLL_ACCESS_COLL_ID" && col.name != "COLL_COLL_ACCESS_COLL_ID") {
                compiler->add_target_type("Collection");
                compiler->add_target_type("CollAccess");
            }
            const auto* gm = find_column_mapping(col.name);
            if (gm) {
                compiler->add_target_type(gm->node_type);
                size_t p_ofs = buf.arr_append_obj(projs_arr_ofs);
                buf.set_str(p_ofs, "alias", gm->node_type);
                buf.set_str(p_ofs, "property", gm->bson_key);
                buf.set_i64(p_ofs, "agg", 0);
                buf.set_str(p_ofs, "as", "idx_" + std::to_string(col_idx++));
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
                    const auto* gm = find_column_mapping(col->name);
                    if (gm) {
                        compiler->add_target_type(gm->node_type);
                        size_t p_ofs = buf.arr_append_obj(projs_arr_ofs);
                        buf.set_str(p_ofs, "alias", gm->node_type);
                        buf.set_str(p_ofs, "property", gm->bson_key);
                        buf.set_i64(p_ofs, "agg", agg);
                        buf.set_bool(p_ofs, "distinct", func.distinct);
                        buf.set_str(p_ofs, "as", "idx_" + std::to_string(col_idx++));
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

    lite3cpp::Buffer Gq2ToL3kvgCompiler::compile(const irods::experimental::genquery2::select& ast, std::string_view override_root_alias, const irods::experimental::genquery2::options* opts) {
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
                     const auto* gm = find_column_mapping(col_name);
                     if (gm) {
                         std::string t = std::string(gm->node_type);
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
                            const auto* gm = find_column_mapping(col.name);
                            if (gm) {
                                std::string t(gm->node_type);
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
                                     const auto* gm = find_column_mapping(col->name);
                                     if (gm) {
                                         std::string t(gm->node_type);
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

        lite3cpp::Buffer qbuf;
        qbuf.init_object();
        qbuf.set_str(0, "root_alias", entry_node_type_);

        // 1. Projections
        size_t projs_ofs = qbuf.set_arr(0, "projections");
        projection_visitor pv(this, qbuf, projs_ofs);
        for (const auto& p : ast.projections) {
            boost::apply_visitor(pv, p);
        }

        // Filters
        lite3cpp::Buffer raw_filters_buf;
        raw_filters_buf.init_array();
        condition_visitor cv(this, raw_filters_buf, 0);
        for(const auto& w : ast.conditions) {
            boost::apply_visitor(cv, w);
        }

        // 2. Groups
        if (!ast.group_by.expressions.empty()) {
            size_t groups_ofs = qbuf.set_arr(0, "groups");
            for (const auto& expr : ast.group_by.expressions) {
                if (const auto* col = std::get_if<irods::experimental::genquery2::column>(&expr)) {
                    const auto* gm = find_column_mapping(col->name);
                    if (!gm) {
                        throw std::invalid_argument("Unknown column: " + col->name);
                    }
                    add_target_type(gm->node_type);
                    size_t g_ofs = qbuf.arr_append_obj(groups_ofs);
                    qbuf.set_str(g_ofs, "alias", gm->node_type);
                    qbuf.set_str(g_ofs, "property", gm->bson_key);
                } else if (const auto* func = std::get_if<irods::experimental::genquery2::function>(&expr)) {
                    std::string fn_name = boost::algorithm::to_upper_copy(func->name);
                    std::string col_alias;
                    std::string col_prop;
                    std::vector<std::string> fn_args;
                    for (const auto& arg : func->arguments) {
                        if (const auto* c = std::get_if<irods::experimental::genquery2::column>(&arg)) {
                            const auto* gm = find_column_mapping(c->name);
                            if (!gm) {
                                throw std::invalid_argument("Unknown column: " + c->name);
                            }
                            add_target_type(gm->node_type);
                            col_alias = gm->node_type;
                            col_prop = gm->bson_key;
                        } else if (const auto* s = std::get_if<std::string>(&arg)) {
                            fn_args.push_back(*s);
                        }
                    }
                    size_t g_ofs = qbuf.arr_append_obj(groups_ofs);
                    qbuf.set_str(g_ofs, "alias", col_alias);
                    qbuf.set_str(g_ofs, "property", col_prop);
                    qbuf.set_str(g_ofs, "func_name", fn_name);
                    size_t fa_ofs = qbuf.set_arr(g_ofs, "func_args");
                    for (const auto& fa : fn_args) {
                        qbuf.arr_append_str(fa_ofs, fa);
                    }
                }
            }
        }

        // 3. Sorts
        if (!ast.order_by.sort_expressions.empty()) {
            size_t sorts_ofs = qbuf.set_arr(0, "sorts");
            for (const auto& se : ast.order_by.sort_expressions) {
                if (const auto* col = std::get_if<irods::experimental::genquery2::column>(&se.expr)) {
                    const auto* gm = find_column_mapping(col->name);
                    if (!gm) {
                        throw std::invalid_argument("Unknown column: " + col->name);
                    }
                    add_target_type(gm->node_type);
                    size_t s_ofs = qbuf.arr_append_obj(sorts_ofs);
                    qbuf.set_str(s_ofs, "alias", gm->node_type);
                    qbuf.set_str(s_ofs, "property", gm->bson_key);
                    qbuf.set_bool(s_ofs, "ascending", se.ascending_order);
                } else if (const auto* func = std::get_if<irods::experimental::genquery2::function>(&se.expr)) {
                    for (const auto& arg : func->arguments) {
                        if (const auto* c = std::get_if<irods::experimental::genquery2::column>(&arg)) {
                            const auto* gm = find_column_mapping(c->name);
                            if (!gm) {
                                throw std::invalid_argument("Unknown column: " + c->name);
                            }
                            add_target_type(gm->node_type);
                            size_t s_ofs = qbuf.arr_append_obj(sorts_ofs);
                            qbuf.set_str(s_ofs, "alias", gm->node_type);
                            qbuf.set_str(s_ofs, "property", gm->bson_key);
                            qbuf.set_bool(s_ofs, "ascending", se.ascending_order);
                            break;
                        }
                    }
                }
            }
        }

        // 4. DataObject / Replica
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

        // 5. Security & Permission filtering
        size_t filters_ofs = qbuf.set_arr(0, "filters");
        bool needs_access_filter = (opts && !opts->admin_mode && !opts->user_name.empty());
        if (needs_access_filter && (has_data_object || has_collection)) {
            std::vector<std::string> access_aliases;
            if (has_data_object) access_aliases.push_back("DataObject");
            if (has_collection) access_aliases.push_back("Collection");

            for (const auto& alias : access_aliases) {
                condition_visitor::append_filter(qbuf, filters_ofs, alias, "_access_user", 0, opts->user_name, "and");
            }
            if (raw_filters_buf.size() >= sizeof(lite3cpp::PackedNodeLayout)) {
                lite3cpp::NodeView rnv(reinterpret_cast<const lite3cpp::PackedNodeLayout*>(raw_filters_buf.data()));
                if (rnv.size() > 0) {
                    size_t grp_ofs = qbuf.arr_append_obj(filters_ofs);
                    qbuf.set_str(grp_ofs, "group", "and");
                    qbuf.set_str(grp_ofs, "prepended_op", "and");
                    size_t sub_arr = qbuf.set_arr(grp_ofs, "filters");
                    for (uint32_t i = 0; i < rnv.size(); ++i) {
                        condition_visitor::copy_filter_element(raw_filters_buf, 0, i, qbuf, sub_arr);
                    }
                }
            }
        } else {
            if (raw_filters_buf.size() >= sizeof(lite3cpp::PackedNodeLayout)) {
                lite3cpp::NodeView rnv(reinterpret_cast<const lite3cpp::PackedNodeLayout*>(raw_filters_buf.data()));
                for (uint32_t i = 0; i < rnv.size(); ++i) {
                    condition_visitor::copy_filter_element(raw_filters_buf, 0, i, qbuf, filters_ofs);
                }
            }
        }

        // 6. Generate steps
        size_t steps_ofs = qbuf.set_arr(0, "steps");
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
                size_t s_ofs = qbuf.arr_append_obj(steps_ofs);
                if (step.dir == Direction::Out) {
                    qbuf.set_str(s_ofs, "type", "out");
                    qbuf.set_str(s_ofs, "label", step.edge_label);
                    qbuf.set_f64(s_ofs, "min_weight", 0.0);
                    qbuf.set_str(s_ofs, "target_alias", step.target_type);
                    qbuf.set_str(s_ofs, "source_alias", current_source);
                } else {
                    qbuf.set_str(s_ofs, "type", "in");
                    qbuf.set_str(s_ofs, "label", step.edge_label);
                    qbuf.set_str(s_ofs, "target_alias", step.target_type);
                    qbuf.set_str(s_ofs, "source_alias", current_source);
                }
                visited.insert(std::string(step.target_type));
                current_source = step.target_type;
            }
        }

        rodsLog(LOG_NOTICE, "L3_COMPILER: Compilation complete.");
        if (!ast.range.number_of_rows.empty()) qbuf.set_i64(0, "limit", std::stoll(ast.range.number_of_rows));
        if (!ast.range.offset.empty()) qbuf.set_i64(0, "offset", std::stoll(ast.range.offset));
        qbuf.set_bool(0, "distinct", ast.distinct);

        return qbuf;
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
