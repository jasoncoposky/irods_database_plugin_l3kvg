#include "irods/catalog/gq2_compiler.hpp"
#include <stdexcept>

namespace irods::catalog::compiler {

    const std::unordered_map<std::string_view, GraphMap> COLUMN_MAP = {
        {"DATA_NAME", {"DataObject", "name"}},
        {"DATA_SIZE", {"DataObject", "size"}},
        {"COLL_NAME", {"Collection", "name"}},
        {"USER_NAME", {"User", "name"}},
        {"RESC_NAME", {"Resource", "name"}}
    };

    template<class... Ts> struct overloaded : Ts... { using Ts::operator()...; };
    template<class... Ts> overloaded(Ts...) -> overloaded<Ts...>;

    Gq2ToL3kvgCompiler::Gq2ToL3kvgCompiler(l3kvg::Engine& engine) 
        : query_(engine.query()) {}

    l3kvg::Query Gq2ToL3kvgCompiler::compile(const std::vector<AstNode>& ast) {
        for (const auto& node : ast) {
            std::visit(overloaded {
                [this](const ConditionNode& n) { compile_condition(n); },
                [this](const SelectNode& n)    { compile_select(n); }
            }, node);
        }
        
        resolve_traversals();
        return query_;
    }

    void Gq2ToL3kvgCompiler::compile_condition(const ConditionNode& cond) {
        auto it = COLUMN_MAP.find(cond.column);
        if (it == COLUMN_MAP.end()) throw std::runtime_error("Unknown column: " + cond.column);
        
        auto map = it->second;
        // Simplified match logic for prototype
        query_.match(map.node_type).where_eq(map.node_type, map.bson_key, cond.value);
        entry_node_type_ = map.node_type;
    }

    void Gq2ToL3kvgCompiler::compile_select(const SelectNode& sel) {
        for (const auto& col : sel.columns) {
            auto it = COLUMN_MAP.find(col);
            if (it == COLUMN_MAP.end()) throw std::runtime_error("Unknown column: " + col);
            
            auto map = it->second;
            target_node_types_.push_back(map.node_type);
            return_fields_.push_back(std::string(map.bson_key));
        }
    }

    void Gq2ToL3kvgCompiler::resolve_traversals() {
        for (size_t i = 0; i < target_node_types_.size(); ++i) {
            auto target = target_node_types_[i];
            if (target != entry_node_type_) {
                auto edge = find_edge(entry_node_type_, target);
                query_.out(edge).as(target);
            }
            query_.return_(target, return_fields_[i]);
        }
    }

    std::string_view Gq2ToL3kvgCompiler::find_edge(std::string_view source_type, std::string_view target_type) {
        if (source_type == "User" && target_type == "DataObject") return "HAS_ACCESS";
        if (source_type == "Collection" && target_type == "DataObject") return "CONTAINS";
        if (source_type == "DataObject" && target_type == "Resource") return "REPLICATED_ON";
        throw std::invalid_argument("No graph path exists between " + std::string(source_type) + " and " + std::string(target_type));
    }

} // namespace irods::catalog::compiler
