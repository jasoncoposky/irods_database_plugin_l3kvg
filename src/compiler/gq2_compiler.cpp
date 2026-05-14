#include "irods/catalog/gq2_compiler.hpp"
#include "irods/rodsGenQuery.h"
#include <stdexcept>
#include <algorithm>
#include <unordered_set>
#include <regex>
#include <iostream>

namespace irods::catalog::compiler {

    static l3kvg::Query::Op parse_operator(std::string_view op_str) {
        if (op_str == "=") return l3kvg::Query::Op::Eq;
        if (op_str == "<>" || op_str == "!=") return l3kvg::Query::Op::Ne;
        if (op_str == ">") return l3kvg::Query::Op::Gt;
        if (op_str == ">=") return l3kvg::Query::Op::Ge;
        if (op_str == "<") return l3kvg::Query::Op::Lt;
        if (op_str == "<=") return l3kvg::Query::Op::Le;
        if (op_str == "LIKE") return l3kvg::Query::Op::Like;
        return l3kvg::Query::Op::Eq;
    }

    struct ParsedCondition {
        l3kvg::Query::Op op;
        std::string value;
    };

    static ParsedCondition parse_condition_string(std::string_view cond) {
        std::string c(cond);
        // Regex to match: [operator] 'value'
        // Example: "= 'foo'", "LIKE '%bar%'", "> 1024"
        std::regex rx(R"(^\s*(LIKE|NOT LIKE|!=|<>|>=|<=|=|>|<)\s*[']?([^']*)[']?\s*$)");
        std::smatch match;
        if (std::regex_search(c, match, rx)) {
            return { parse_operator(match[1].str()), match[2].str() };
        }
        
        // No operator found, assume exact match on the whole string
        // Trim optional single quotes if present
        std::string val = c;
        if (val.size() >= 2 && val.front() == '\'' && val.back() == '\'') {
            val = val.substr(1, val.size() - 2);
        }
        return { l3kvg::Query::Op::Eq, val };
    }

    const std::unordered_map<int, GraphMap> COLUMN_MAP = {
        // Data Objects
        {COL_D_DATA_ID, {"DataObject", "id"}},
        {COL_D_COLL_ID, {"DataObject", "coll_id"}},
        {COL_DATA_NAME, {"DataObject", "name"}},
        {COL_DATA_REPL_NUM, {"DataObject", "repl_num"}},
        {COL_DATA_SIZE, {"DataObject", "size"}},
        {COL_D_RESC_NAME, {"DataObject", "resc_name"}},
        {COL_D_DATA_PATH, {"DataObject", "path"}},
        {COL_D_OWNER_NAME, {"DataObject", "owner"}},
        {COL_D_OWNER_ZONE, {"DataObject", "owner_zone"}},
        {COL_D_REPL_STATUS, {"DataObject", "repl_status"}},
        {COL_D_DATA_CHECKSUM, {"DataObject", "checksum"}},
        {COL_D_CREATE_TIME, {"DataObject", "create_ts"}},
        {COL_D_MODIFY_TIME, {"DataObject", "modify_ts"}},
        {COL_D_RESC_HIER, {"DataObject", "resc_hier"}},
        {COL_D_RESC_ID, {"DataObject", "resc_id"}},

        // Collections
        {COL_COLL_ID, {"Collection", "id"}},
        {COL_COLL_NAME, {"Collection", "name"}},
        {COL_COLL_PARENT_NAME, {"Collection", "parent_name"}},
        {COL_COLL_OWNER_NAME, {"Collection", "owner"}},
        {COL_COLL_OWNER_ZONE, {"Collection", "owner_zone"}},
        {COL_COLL_CREATE_TIME, {"Collection", "create_ts"}},
        {COL_COLL_MODIFY_TIME, {"Collection", "modify_ts"}},

        // Users
        {COL_USER_ID, {"User", "id"}},
        {COL_USER_NAME, {"User", "name"}},
        {COL_USER_TYPE, {"User", "type"}},
        {COL_USER_ZONE, {"User", "zone"}},
        {COL_USER_CREATE_TIME, {"User", "create_ts"}},
        {COL_USER_MODIFY_TIME, {"User", "modify_ts"}},

        // Resources
        {COL_R_RESC_ID, {"Resource", "id"}},
        {COL_R_RESC_NAME, {"Resource", "name"}},
        {COL_R_TYPE_NAME, {"Resource", "type"}},
        {COL_R_LOC, {"Resource", "location"}},
        {COL_R_VAULT_PATH, {"Resource", "vault_path"}},
        {308, {"Resource", "context"}}, // COL_R_RESC_CONTEXT
        {309, {"Resource", "parent"}},  // COL_R_RESC_PARENT
        {COL_R_CREATE_TIME, {"Resource", "create_ts"}},
        {COL_R_MODIFY_TIME, {"Resource", "modify_ts"}},

        // Metadata (AVUs)
        {COL_META_DATA_ATTR_NAME, {"AVU", "attribute"}},
        {COL_META_DATA_ATTR_VALUE, {"AVU", "value"}},
        {COL_META_DATA_ATTR_UNITS, {"AVU", "unit"}},
        {COL_META_COLL_ATTR_NAME, {"AVU", "attribute"}},
        {COL_META_COLL_ATTR_VALUE, {"AVU", "value"}},
        {COL_META_COLL_ATTR_UNITS, {"AVU", "unit"}},
        {COL_META_DATA_CREATE_TIME, {"AVU", "create_ts"}},
        {COL_META_DATA_MODIFY_TIME, {"AVU", "modify_ts"}},

        // Zones
        {COL_ZONE_ID, {"Zone", "id"}},
        {COL_ZONE_NAME, {"Zone", "name"}},
        {COL_ZONE_TYPE, {"Zone", "type"}},
        {COL_ZONE_CONNECTION, {"Zone", "connection"}},

        // Access Control
        {COL_DATA_ACCESS_NAME, {"Access", "level"}},
        {COL_DATA_TOKEN_NAMESPACE, {"Access", "namespace"}}
    };

    const std::unordered_map<std::string_view, GraphMap> COLUMN_NAME_MAP = {
        {"DATA_ID", {"DataObject", "id"}},
        {"DATA_NAME", {"DataObject", "name"}},
        {"DATA_SIZE", {"DataObject", "size"}},
        {"COLL_ID", {"Collection", "id"}},
        {"COLL_NAME", {"Collection", "name"}},
        {"USER_NAME", {"User", "name"}},
        {"RESC_NAME", {"Resource", "name"}},
        {"META_DATA_ATTR_NAME", {"AVU", "attribute"}},
        {"META_DATA_ATTR_VALUE", {"AVU", "value"}},
        {"META_DATA_ATTR_UNIT", {"AVU", "unit"}},
        {"META_COLL_ATTR_NAME", {"AVU", "attribute"}},
        {"META_COLL_ATTR_VALUE", {"AVU", "value"}},
        {"META_COLL_ATTR_UNIT", {"AVU", "unit"}},
        {"DATA_ACCESS_NAME", {"Access", "level"}}
    };

    using Dir = Gq2ToL3kvgCompiler::PathStep::Direction;
    using RouteKey = std::pair<std::string_view, std::string_view>;
    
    struct RouteKeyHash {
        std::size_t operator()(const RouteKey& k) const {
            return std::hash<std::string_view>()(k.first) ^ (std::hash<std::string_view>()(k.second) << 1);
        }
    };

    static const std::unordered_map<RouteKey, std::vector<Gq2ToL3kvgCompiler::PathStep>, RouteKeyHash> ROUTING_TABLE = {
        // Multi-Hop Paths
        {{"Collection", "Resource"}, {{Dir::Out, "CONTAINS", "DataObject"}, {Dir::Out, "REPLICATED_ON", "Resource"}}},
        {{"User", "Resource"}, {{Dir::Out, "HAS_ACCESS", "Access"}, {Dir::Out, "FOR_OBJECT", "DataObject"}, {Dir::Out, "REPLICATED_ON", "Resource"}}},
        {{"User", "Collection"}, {{Dir::Out, "HAS_ACCESS", "Access"}, {Dir::Out, "FOR_OBJECT", "DataObject"}, {Dir::In, "CONTAINS", "Collection"}}},
        {{"User", "DataObject"}, {{Dir::Out, "HAS_ACCESS", "Access"}, {Dir::Out, "FOR_OBJECT", "DataObject"}}},
        {{"DataObject", "Resource"}, {{Dir::Out, "REPLICATED_ON", "Resource"}}},
        
        // Metadata Paths
        {{"DataObject", "AVU"}, {{Dir::Out, "ANNOTATED_WITH", "AVU"}}},
        {{"AVU", "DataObject"}, {{Dir::In, "ANNOTATED_WITH", "DataObject"}}},
        {{"AVU", "Collection"}, {{Dir::In, "ANNOTATED_WITH", "Collection"}}},
        
        // Zone Paths
        {{"Zone", "User"}, {{Dir::Out, "HAS_USER", "User"}}},
        {{"Zone", "Collection"}, {{Dir::Out, "HAS_ROOT_COLL", "Collection"}}},
        {{"Zone", "Resource"}, {{Dir::Out, "HAS_RESC", "Resource"}}},
        
        // Replica Paths
        {{"DataObject", "Replica"}, {{Dir::Out, "HAS_REPLICA", "Replica"}}},
        {{"Replica", "Resource"}, {{Dir::Out, "STAYING_AT", "Resource"}}},
        {{"Replica", "AVU"}, {{Dir::Out, "ANNOTATED_WITH", "AVU"}}},

        // Single Hop Defaults / Fallbacks
        {{"User", "Access"}, {{Dir::Out, "HAS_ACCESS", "Access"}}},
        {{"Access", "DataObject"}, {{Dir::Out, "FOR_OBJECT", "DataObject"}}},
        {{"Collection", "DataObject"}, {{Dir::Out, "CONTAINS", "DataObject"}}},
        {{"DataObject", "AVU"}, {{Dir::Out, "ANNOTATED_WITH", "AVU"}}},
        {{"Collection", "Collection"}, {{Dir::Out, "CONTAINS", "Collection"}}}
    };

    template<class... Ts> struct overloaded : Ts... { using Ts::operator()...; };
    template<class... Ts> overloaded(Ts...) -> overloaded<Ts...>;

    struct condition_visitor : public boost::static_visitor<std::pair<l3kvg::Query::Op, std::string>> {
        std::pair<l3kvg::Query::Op, std::string> operator()(const irods::experimental::genquery2::condition_equal& c) const { return {l3kvg::Query::Op::Eq, c.string_literal}; }
        std::pair<l3kvg::Query::Op, std::string> operator()(const irods::experimental::genquery2::condition_not_equal& c) const { return {l3kvg::Query::Op::Ne, c.string_literal}; }
        std::pair<l3kvg::Query::Op, std::string> operator()(const irods::experimental::genquery2::condition_less_than& c) const { return {l3kvg::Query::Op::Lt, c.string_literal}; }
        std::pair<l3kvg::Query::Op, std::string> operator()(const irods::experimental::genquery2::condition_less_than_or_equal_to& c) const { return {l3kvg::Query::Op::Le, c.string_literal}; }
        std::pair<l3kvg::Query::Op, std::string> operator()(const irods::experimental::genquery2::condition_greater_than& c) const { return {l3kvg::Query::Op::Gt, c.string_literal}; }
        std::pair<l3kvg::Query::Op, std::string> operator()(const irods::experimental::genquery2::condition_greater_than_or_equal_to& c) const { return {l3kvg::Query::Op::Ge, c.string_literal}; }
        std::pair<l3kvg::Query::Op, std::string> operator()(const irods::experimental::genquery2::condition_like& c) const { return {l3kvg::Query::Op::Like, c.string_literal}; }
        template<typename T> std::pair<l3kvg::Query::Op, std::string> operator()(const T&) const { return {l3kvg::Query::Op::Eq, ""}; }
    };

    struct condition_tree_visitor : public boost::static_visitor<void> {
        Gq2ToL3kvgCompiler* compiler;
        l3kvg::Query::FilterGroup& group;
        bool use_or;

        condition_tree_visitor(Gq2ToL3kvgCompiler* c, l3kvg::Query::FilterGroup& g, bool o = false)
            : compiler(c), group(g), use_or(o) {}

        void operator()(const irods::experimental::genquery2::condition& cond) const {
            if (auto* col = std::get_if<irods::experimental::genquery2::column>(&cond.lhs)) {
                auto it = COLUMN_NAME_MAP.find(col->name);
                if (it != COLUMN_NAME_MAP.end()) {
                    condition_visitor v;
                    auto [op, val] = boost::apply_visitor(v, cond.expression);
                    if (use_or) group.or_where(it->second.node_type, it->second.bson_key, op, val);
                    else group.where(it->second.node_type, it->second.bson_key, op, val);
                }
            }
        }

        void operator()(const irods::experimental::genquery2::logical_and& la) const {
            auto cb = [&](l3kvg::Query::FilterGroup& sub) {
                for (const auto& w : la.condition) {
                    boost::apply_visitor(condition_tree_visitor(compiler, sub, false), w);
                }
            };
            if (use_or) group.or_where_group(cb);
            else group.where_group(cb);
        }

        void operator()(const irods::experimental::genquery2::logical_or& lo) const {
            auto cb = [&](l3kvg::Query::FilterGroup& sub) {
                bool first = true;
                for (const auto& w : lo.condition) {
                    boost::apply_visitor(condition_tree_visitor(compiler, sub, !first), w);
                    first = false;
                }
            };
            if (use_or) group.or_where_group(cb);
            else group.where_group(cb);
        }

        void operator()(const irods::experimental::genquery2::logical_grouping& lg) const {
            auto cb = [&](l3kvg::Query::FilterGroup& sub) {
                for (const auto& w : lg.conditions) {
                    boost::apply_visitor(condition_tree_visitor(compiler, sub, false), w);
                }
            };
            if (use_or) group.or_where_group(cb);
            else group.where_group(cb);
        }

        void operator()(const irods::experimental::genquery2::logical_not& ln) const {}
    };

    Gq2ToL3kvgCompiler::Gq2ToL3kvgCompiler(l3kvg::Engine& engine) 
        : query_(engine.query()) {}

    l3kvg::Query Gq2ToL3kvgCompiler::compile(const irods::experimental::genquery2::select& ast) {
        namespace gq = irods::experimental::genquery2;

        // 1. Process Projections (Select)
        struct projection_visitor : public boost::static_visitor<void> {
            Gq2ToL3kvgCompiler* compiler;
            projection_visitor(Gq2ToL3kvgCompiler* c) : compiler(c) {}
            void operator()(const gq::column& col) const {
                auto it = COLUMN_NAME_MAP.find(col.name);
                if (it != COLUMN_NAME_MAP.end()) {
                    compiler->target_node_types_.push_back(it->second.node_type);
                    compiler->return_fields_.push_back({std::string(it->second.bson_key), l3kvg::Query::AggOp::None});
                }
            }
            void operator()(const gq::function& fn) const { 
                l3kvg::Query::AggOp agg = l3kvg::Query::AggOp::None;
                if (fn.name == "COUNT") agg = l3kvg::Query::AggOp::Count;
                else if (fn.name == "SUM") agg = l3kvg::Query::AggOp::Sum;
                else if (fn.name == "AVG") agg = l3kvg::Query::AggOp::Avg;
                else if (fn.name == "MIN") agg = l3kvg::Query::AggOp::Min;
                else if (fn.name == "MAX") agg = l3kvg::Query::AggOp::Max;

                if (!fn.arguments.empty()) {
                    if (auto* col = std::get_if<gq::column>(&fn.arguments[0])) {
                        auto it = COLUMN_NAME_MAP.find(col->name);
                        if (it != COLUMN_NAME_MAP.end()) {
                            compiler->target_node_types_.push_back(it->second.node_type);
                            compiler->return_fields_.push_back({std::string(it->second.bson_key), agg});
                        }
                    }
                }
            }
        };
        projection_visitor proj_v(this);
        for (const auto& proj : ast.projections) {
            boost::apply_visitor(proj_v, proj);
        }

        // 2. Select Anchor
        struct anchor_visitor : public boost::static_visitor<void> {
             Gq2ToL3kvgCompiler* compiler;
             anchor_visitor(Gq2ToL3kvgCompiler* c) : compiler(c) {}
             void operator()(const gq::condition& c) {
                 if (auto* col = std::get_if<gq::column>(&c.lhs)) {
                     if (compiler->entry_node_type_.empty()) {
                         auto it = COLUMN_NAME_MAP.find(col->name);
                         if (it != COLUMN_NAME_MAP.end()) {
                             compiler->entry_node_type_ = it->second.node_type;
                         }
                     }
                 }
             }
             void operator()(const gq::logical_and& la) { for(const auto& w : la.condition) boost::apply_visitor(*this, w); }
             void operator()(const gq::logical_or& lo) { for(const auto& w : lo.condition) boost::apply_visitor(*this, w); }
             void operator()(const gq::logical_grouping& lg) { for(const auto& w : lg.conditions) boost::apply_visitor(*this, w); }
             void operator()(const gq::logical_not& ln) {}
        };
        anchor_visitor av(this);
        for(const auto& w : ast.conditions) boost::apply_visitor(av, w);

        if (entry_node_type_.empty()) entry_node_type_ = "DataObject";
        query_.match(entry_node_type_);

        // 3. Process Conditions recursively
        for (const auto& wrap : ast.conditions) {
            boost::apply_visitor(condition_tree_visitor(this, query_.get_root_filters()), wrap);
        }

        // 4. Process Sorting
        for (const auto& sort : ast.order_by.sort_expressions) {
            if (auto* col = std::get_if<gq::column>(&sort.expr)) {
                auto it = COLUMN_NAME_MAP.find(col->name);
                if (it != COLUMN_NAME_MAP.end()) {
                    query_.order_by(it->second.node_type, it->second.bson_key, sort.ascending_order);
                }
            }
        }

        // 5. Process Pagination
        if (!ast.range.number_of_rows.empty()) {
            try { query_.limit(std::stoul(ast.range.number_of_rows)); } catch(...) {}
        }
        if (!ast.range.offset.empty()) {
            try { query_.offset(std::stoul(ast.range.offset)); } catch(...) {}
        }

        // 6. Process Grouping
        for (const auto& expr : ast.group_by.expressions) {
            if (auto* col = std::get_if<gq::column>(&expr)) {
                auto it = COLUMN_NAME_MAP.find(col->name);
                if (it != COLUMN_NAME_MAP.end()) {
                    query_.group_by(it->second.node_type, it->second.bson_key);
                }
            }
        }

        resolve_traversals();
        return query_;
    }

    void Gq2ToL3kvgCompiler::resolve_traversals() {
        std::unordered_set<std::string_view> traversed_aliases;
        traversed_aliases.insert(entry_node_type_);

        for (size_t i = 0; i < target_node_types_.size(); ++i) {
            auto target = target_node_types_[i];
            if (target == entry_node_type_) {
                 query_.return_(target, return_fields_[i].bson_key, return_fields_[i].agg);
                 continue;
            }

            if (!traversed_aliases.contains(target)) {
                auto path = find_path(entry_node_type_, target);
                std::string_view current_source = entry_node_type_;
                
                for (const auto& step : path) {
                    if (!traversed_aliases.contains(step.target_type)) {
                        if (step.dir == Gq2ToL3kvgCompiler::PathStep::Direction::Out) {
                            query_.out(step.edge_label).as(step.target_type);
                        } else {
                            query_.in(step.edge_label).as(step.target_type);
                        }
                        traversed_aliases.insert(step.target_type);
                    }
                    current_source = step.target_type;
                }
            }
            query_.return_(target, return_fields_[i].bson_key, return_fields_[i].agg);
        }
    }

    std::vector<Gq2ToL3kvgCompiler::PathStep> Gq2ToL3kvgCompiler::find_path(std::string_view source, std::string_view target) {
        if (source == target) return {};
        
        auto it = ROUTING_TABLE.find({source, target});
        if (it != ROUTING_TABLE.end()) return it->second;
        
        // Fallback to find_edge if no multi-hop defined
        return {{Dir::Out, find_edge(source, target), target}};
    }

    std::string_view Gq2ToL3kvgCompiler::find_edge(std::string_view source_type, std::string_view target_type) {
        auto it = ROUTING_TABLE.find({source_type, target_type});
        if (it != ROUTING_TABLE.end() && it->second.size() == 1) {
            return it->second[0].edge_label;
        }
        
        throw std::invalid_argument("No graph edge exists between " + std::string(source_type) + " and " + std::string(target_type));
    }

} // namespace irods::catalog::compiler
