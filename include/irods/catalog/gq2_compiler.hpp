#pragma once

#include <string>
#include <vector>
#include <variant>
#include <unordered_map>
#include "L3KVG/Query.hpp"

namespace irods::catalog::compiler {

    struct GraphMap {
        std::string_view node_type;
        std::string_view bson_key;
    };

    struct ConditionNode {
        std::string column;
        std::string op;
        std::string value;
    };

    struct SelectNode {
        std::vector<std::string> columns;
    };

    using AstNode = std::variant<ConditionNode, SelectNode>;

    class Gq2ToL3kvgCompiler {
    public:
        explicit Gq2ToL3kvgCompiler(l3kvg::Engine& engine);
        
        l3kvg::Query compile(const std::vector<AstNode>& ast);

    private:
        l3kvg::Query query_;
        std::string_view entry_node_type_;
        std::vector<std::string_view> target_node_types_;
        std::vector<std::string> return_fields_;

        void compile_condition(const ConditionNode& cond);
        void compile_select(const SelectNode& sel);
        void resolve_traversals();
        
        std::string_view find_edge(std::string_view source_type, std::string_view target_type);
    };

} // namespace irods::catalog::compiler
