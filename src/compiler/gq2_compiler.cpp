#include "irods/catalog/gq2_compiler.hpp"
#include "irods/rodsGenQuery.h"
#include <nlohmann/json.hpp>
#include <stdexcept>
#include <algorithm>
#include <unordered_set>
#include <regex>
#include <iostream>

using json = nlohmann::json;

namespace irods::catalog::compiler {

    const std::unordered_map<int, GraphMap> COLUMN_MAP = {
        // DataObject
        {COL_D_DATA_ID,        {"DataObject", "id"}},
        {COL_D_COLL_ID,        {"DataObject", "coll_id"}},
        {COL_DATA_NAME,        {"DataObject", "n"}},
        {COL_DATA_REPL_NUM,    {"Replica",    "rn"}},
        {COL_DATA_VERSION,     {"DataObject", "v"}},
        {COL_DATA_TYPE_NAME,   {"DataObject", "t"}},
        {COL_DATA_SIZE,        {"DataObject", "s"}},
        {COL_DATA_MODE,        {"DataObject", "mode"}},
        {COL_D_RESC_NAME,      {"DataObject", "resc_name"}},
        {COL_D_RESC_HIER,      {"Replica",    "h"}},
        {COL_D_DATA_PATH,      {"Replica",    "p"}},
        {COL_D_OWNER_NAME,     {"DataObject", "o"}},
        {COL_D_OWNER_ZONE,     {"DataObject", "z"}},
        {COL_D_REPL_STATUS,    {"Replica",    "s"}},
        {COL_D_DATA_STATUS,    {"DataObject", "st"}},
        {COL_D_DATA_CHECKSUM,  {"DataObject", "cs"}},
        {COL_D_EXPIRY,         {"DataObject", "expiry"}},
        {COL_D_COMMENTS,       {"DataObject", "m"}},
        {COL_D_CREATE_TIME,    {"DataObject", "ct"}},
        {COL_D_MODIFY_TIME,    {"DataObject", "mt"}},
        {COL_D_RESC_ID,        {"Replica",    "resc_id"}},
        {COL_D_ACCESS_TIME,    {"Replica",    "at"}},

        // Collection
        {COL_COLL_ID,          {"Collection", "id"}},
        {COL_COLL_NAME,        {"Collection", "n"}},
        {COL_COLL_PARENT_NAME, {"Collection", "parent_name"}},
        {COL_COLL_OWNER_NAME,  {"Collection", "o"}},
        {COL_COLL_OWNER_ZONE,  {"Collection", "z"}},
        {COL_COLL_INHERITANCE, {"Collection", "i"}},
        {COL_COLL_COMMENTS,    {"Collection", "m"}},
        {COL_COLL_CREATE_TIME, {"Collection", "ct"}},
        {COL_COLL_MODIFY_TIME, {"Collection", "mt"}},
        {COL_COLL_TYPE,        {"Collection", "t"}},
        {COL_COLL_INFO1,       {"Collection", "c1"}},
        {COL_COLL_INFO2,       {"Collection", "c2"}},

        // User
        {COL_USER_ID,          {"User", "id"}},
        {COL_USER_NAME,        {"User", "n"}},
        {COL_USER_TYPE,        {"User", "t"}},
        {COL_USER_ZONE,        {"User", "z"}},
        {COL_USER_DN,          {"User", "d"}},
        {COL_USER_INFO,        {"User", "i"}},
        {COL_USER_COMMENT,     {"User", "m"}},
        {COL_USER_CREATE_TIME, {"User", "ct"}},
        {COL_USER_MODIFY_TIME, {"User", "mt"}},

        // Resource
        {COL_R_RESC_ID,        {"Resource", "id"}},
        {COL_R_RESC_NAME,      {"Resource", "n"}},
        {COL_R_ZONE_NAME,      {"Resource", "z"}},
        {COL_R_TYPE_NAME,      {"Resource", "t"}},
        {COL_R_LOC,            {"Resource", "l"}},
        {COL_R_VAULT_PATH,     {"Resource", "v"}},
        {COL_R_FREE_SPACE,     {"Resource", "f"}},
        {COL_R_RESC_STATUS,    {"Resource", "s"}},
        {COL_R_RESC_CONTEXT,   {"Resource", "c"}},
        {COL_R_RESC_COMMENT,   {"Resource", "m"}},
        {COL_R_CREATE_TIME,    {"Resource", "ct"}},
        {COL_R_MODIFY_TIME,    {"Resource", "mt"}},

        // Metadata
        {COL_META_DATA_ATTR_NAME,  {"Metadata", "a"}},
        {COL_META_DATA_ATTR_VALUE, {"Metadata", "v"}},
        {COL_META_DATA_ATTR_UNITS, {"Metadata", "u"}},
        {COL_META_COLL_ATTR_NAME,  {"Metadata", "a"}},
        {COL_META_COLL_ATTR_VALUE, {"Metadata", "v"}},
        {COL_META_COLL_ATTR_UNITS, {"Metadata", "u"}},

        // Zone
        {COL_ZONE_ID,          {"Zone", "id"}},
        {COL_ZONE_NAME,        {"Zone", "n"}},
        {COL_ZONE_TYPE,        {"Zone", "t"}},
        {COL_ZONE_CONNECTION,  {"Zone", "c"}},
        {COL_ZONE_COMMENT,     {"Zone", "m"}},

        // Access
        {COL_DATA_ACCESS_NAME,     {"Access", "l"}},
        {COL_COLL_ACCESS_NAME,     {"Access", "l"}},
        {COL_DATA_TOKEN_NAMESPACE, {"Access", "n"}}
    };

    const std::unordered_map<std::string_view, GraphMap> COLUMN_NAME_MAP = {
        {"DATA_ID",        {"DataObject", "id"}},
        {"DATA_NAME",      {"DataObject", "n"}},
        {"DATA_SIZE",      {"DataObject", "s"}},
        {"DATA_REPL_NUM",  {"Replica",    "rn"}},
        {"DATA_RESC_NAME", {"DataObject", "resc_name"}},
        {"DATA_PATH",      {"Replica",    "p"}},
        {"DATA_OWNER_NAME",{"DataObject", "o"}},
        {"DATA_OWNER_ZONE",{"DataObject", "z"}},
        {"DATA_REPL_STATUS",{"Replica",    "s"}},
        {"DATA_CHECKSUM",  {"DataObject", "cs"}},
        {"DATA_CREATE_TIME",{"DataObject", "ct"}},
        {"DATA_MODIFY_TIME",{"DataObject", "mt"}},
        
        {"COLL_ID",        {"Collection", "id"}},
        {"COLL_NAME",      {"Collection", "n"}},
        {"COLL_OWNER_NAME",{"Collection", "o"}},
        {"COLL_OWNER_ZONE",{"Collection", "z"}},
        {"COLL_CREATE_TIME",{"Collection", "ct"}},
        {"COLL_MODIFY_TIME",{"Collection", "mt"}},
        {"COLL_ACCESS_NAME",{"Access", "l"}},
        
        {"USER_ID",        {"User", "id"}},
        {"USER_NAME",      {"User", "n"}},
        {"USER_TYPE",      {"User", "t"}},
        {"USER_ZONE",      {"User", "z"}},
        {"USER_CREATE_TIME",{"User", "ct"}},
        {"USER_MODIFY_TIME",{"User", "mt"}},
        
        {"RESC_ID",        {"Resource", "id"}},
        {"RESC_NAME",      {"Resource", "n"}},
        {"RESC_TYPE_NAME", {"Resource", "t"}},
        {"RESC_LOC",       {"Resource", "l"}},
        {"RESC_VAULT_PATH",{"Resource", "v"}},
        {"RESC_CREATE_TIME",{"Resource", "ct"}},
        {"RESC_MODIFY_TIME",{"Resource", "mt"}},
        
        {"META_DATA_ATTR_NAME", {"Metadata", "a"}},
        {"META_DATA_ATTR_VALUE",{"Metadata", "v"}},
        {"META_DATA_ATTR_UNITS",{"Metadata", "u"}},

        {"ZONE_ID",        {"Zone", "id"}},
        {"ZONE_NAME",      {"Zone", "n"}},
        {"ZONE_TYPE",      {"Zone", "t"}},
        {"ZONE_CONNECTION",{"Zone", "c"}},

        {"DATA_ACCESS_NAME", {"Access", "l"}},
        {"COLL_ACCESS_NAME", {"Access", "l"}},
        {"DATA_TOKEN_NAMESPACE", {"Access", "n"}},
        {"COLL_TOKEN_NAMESPACE", {"Access", "n"}}
    };

    using Dir = Gq2ToL3kvgCompiler::PathStep::Direction;
    using RouteKey = std::pair<std::string_view, std::string_view>;
    
    struct RouteKeyHash {
        std::size_t operator()(const RouteKey& k) const {
            return std::hash<std::string_view>()(k.first) ^ (std::hash<std::string_view>()(k.second) << 1);
        }
    };

    static const std::unordered_map<RouteKey, std::vector<Gq2ToL3kvgCompiler::PathStep>, RouteKeyHash> ROUTING_TABLE = {
        {{"DataObject", "Collection"}, {{Dir::In, "CONTAINS", "Collection"}}},
        {{"DataObject", "Resource"},   {{Dir::Out, "HAS_REPLICA", "Replica"}, {Dir::Out, "STAYING_AT", "Resource"}}},
        {{"DataObject", "Metadata"},   {{Dir::Out, "ANNOTATED_WITH", "Metadata"}}},
        {{"DataObject", "Replica"},    {{Dir::Out, "HAS_REPLICA", "Replica"}}},
        {{"DataObject", "Access"},     {{Dir::In, "FOR_OBJECT", "Access"}}},
        {{"DataObject", "User"},       {{Dir::In, "FOR_OBJECT", "Access"}, {Dir::In, "HAS_ACCESS", "User"}}},
        {{"DataObject", "Zone"},       {{Dir::Out, "HAS_REPLICA", "Replica"}, {Dir::Out, "STAYING_AT", "Resource"}, {Dir::In, "HAS_RESC", "Zone"}}},
        {{"Collection", "DataObject"}, {{Dir::Out, "CONTAINS", "DataObject"}}},
        {{"Collection", "Resource"},   {{Dir::Out, "CONTAINS", "DataObject"}, {Dir::Out, "HAS_REPLICA", "Replica"}, {Dir::Out, "STAYING_AT", "Resource"}}},
        {{"Collection", "Metadata"},   {{Dir::Out, "ANNOTATED_WITH", "Metadata"}}},
        {{"Collection", "Collection"}, {{Dir::Out, "CONTAINS", "Collection"}}},
        {{"Collection", "Access"},     {{Dir::In, "FOR_OBJECT", "Access"}}},
        {{"Collection", "User"},       {{Dir::In, "FOR_OBJECT", "Access"}, {Dir::In, "HAS_ACCESS", "User"}}},
        {{"Collection", "Zone"},       {{Dir::In, "HAS_ROOT_COLL", "Zone"}}},
        {{"User", "Access"},           {{Dir::Out, "HAS_ACCESS", "Access"}}},
        {{"User", "DataObject"},       {{Dir::Out, "HAS_ACCESS", "Access"}, {Dir::Out, "FOR_OBJECT", "DataObject"}}},
        {{"User", "Collection"},       {{Dir::Out, "HAS_ACCESS", "Access"}, {Dir::Out, "FOR_OBJECT", "DataObject"}, {Dir::In, "CONTAINS", "Collection"}}},
        {{"User", "Resource"},         {{Dir::Out, "HAS_ACCESS", "Access"}, {Dir::Out, "FOR_OBJECT", "DataObject"}, {Dir::Out, "HAS_REPLICA", "Replica"}, {Dir::Out, "STAYING_AT", "Resource"}}},
        {{"User", "Zone"},             {{Dir::In, "HAS_USER", "Zone"}}},
        {{"Access", "User"},           {{Dir::In, "HAS_ACCESS", "User"}}},
        {{"Access", "DataObject"},     {{Dir::Out, "FOR_OBJECT", "DataObject"}}},
        {{"Access", "Collection"},     {{Dir::Out, "FOR_OBJECT", "Collection"}}},
        {{"Metadata", "DataObject"},   {{Dir::In, "ANNOTATED_WITH", "DataObject"}}},
        {{"Metadata", "Collection"},   {{Dir::In, "ANNOTATED_WITH", "Collection"}}},
        {{"Metadata", "User"},         {{Dir::In, "ANNOTATED_WITH", "User"}}},
        {{"Metadata", "Resource"},     {{Dir::In, "ANNOTATED_WITH", "Resource"}}},
        {{"Resource", "DataObject"},   {{Dir::In, "STAYING_AT", "Replica"}, {Dir::In, "HAS_REPLICA", "DataObject"}}},
        {{"Resource", "Replica"},      {{Dir::In, "STAYING_AT", "Replica"}}},
        {{"Resource", "Zone"},         {{Dir::In, "HAS_RESC", "Zone"}}}
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
        projection_visitor(Gq2ToL3kvgCompiler* c, json& jp) : compiler(c), j_projs(jp) {}

        void operator()(const irods::experimental::genquery2::column& col) const {
            auto it = COLUMN_NAME_MAP.find(col.name);
            if (it != COLUMN_NAME_MAP.end()) {
                compiler->add_target_type(it->second.node_type);
                j_projs.push_back({{"alias", it->second.node_type}, {"property", it->second.bson_key}, {"agg", 0}});
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
                        j_projs.push_back({{"alias", it->second.node_type}, {"property", it->second.bson_key}, {"agg", agg}});
                    }
                }
            }
        }
    };

    std::string Gq2ToL3kvgCompiler::compile(const irods::experimental::genquery2::select& ast) {
        json j;
        json j_projs = json::array();
        projection_visitor pv(this, j_projs);
        for (const auto& p : ast.projections) {
            boost::apply_visitor(pv, p);
        }
        j["projections"] = j_projs;

        struct anchor_visitor : public boost::static_visitor<void> {
             Gq2ToL3kvgCompiler* compiler;
             anchor_visitor(Gq2ToL3kvgCompiler* c) : compiler(c) {}
             void operator()(const irods::experimental::genquery2::condition& c) {
                 std::string col_name;
                 if (auto* col = std::get_if<irods::experimental::genquery2::column>(&c.lhs)) col_name = col->name;
                 auto it = COLUMN_NAME_MAP.find(col_name);
                 if (it != COLUMN_NAME_MAP.end()) compiler->set_entry_type(it->second.node_type);
             }
             void operator()(const irods::experimental::genquery2::logical_and& l) { for(const auto& c : l.condition) boost::apply_visitor(*this, c); }
             void operator()(const irods::experimental::genquery2::logical_or& l) { for(const auto& c : l.condition) boost::apply_visitor(*this, c); }
             void operator()(const irods::experimental::genquery2::logical_grouping& l) { for(const auto& c : l.conditions) boost::apply_visitor(*this, c); }
             void operator()(const irods::experimental::genquery2::logical_not& l) { for(const auto& c : l.condition) boost::apply_visitor(*this, c); }
        };
        anchor_visitor av(this);
        for(const auto& w : ast.conditions) boost::apply_visitor(av, w);

        if (entry_node_type_.empty()) entry_node_type_ = "DataObject";
        j["root_alias"] = entry_node_type_;

        json j_steps = json::array();
        std::unordered_set<std::string_view> visited = { entry_node_type_ };
        for (const auto& target : target_node_types_) {
            if (visited.count(target)) continue;
            auto path = find_path(entry_node_type_, target);
            for (const auto& step : path) {
                if (step.dir == Dir::Out) j_steps.push_back({{"type", "out"}, {"label", step.edge_label}, {"min_weight", 0.0}, {"target_alias", step.target_type}});
                else j_steps.push_back({{"type", "in"}, {"label", step.edge_label}, {"target_alias", step.target_type}});
                visited.insert(step.target_type);
            }
        }
        j["steps"] = j_steps;

        json j_filters = json::array();
        condition_visitor cv(this, j_filters);
        for(const auto& w : ast.conditions) boost::apply_visitor(cv, w);
        j["filters"] = j_filters;

        if (!ast.range.number_of_rows.empty()) j["limit"] = std::stoull(ast.range.number_of_rows);
        if (!ast.range.offset.empty()) j["offset"] = std::stoull(ast.range.offset);
        j["distinct"] = ast.distinct;

        return j.dump();
    }

    std::vector<Gq2ToL3kvgCompiler::PathStep> Gq2ToL3kvgCompiler::find_path(std::string_view source, std::string_view target) {
        auto it = ROUTING_TABLE.find({source, target});
        if (it != ROUTING_TABLE.end()) return it->second;
        return {};
    }

} // namespace irods::catalog::compiler
