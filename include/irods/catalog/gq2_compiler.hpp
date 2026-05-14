#pragma once

#include <string>
#include <vector>
#include <variant>
#include <unordered_map>
#include "L3KVG/Query.hpp"
#include "irods/private/genquery2_ast_types.hpp"

namespace irods::catalog::compiler {

    struct GraphMap {
        std::string_view node_type;
        std::string_view bson_key;
    };

    extern const std::unordered_map<int, GraphMap> COLUMN_MAP;
    extern const std::unordered_map<std::string_view, GraphMap> COLUMN_NAME_MAP;

    class Gq2ToL3kvgCompiler {
    public:
        explicit Gq2ToL3kvgCompiler(l3kvg::Engine& engine);
        
        l3kvg::Query compile(const irods::experimental::genquery2::select& ast);

        struct PathStep {
            enum class Direction { Out, In };
            Direction dir;
            std::string_view edge_label;
            std::string_view target_type;
        };

    private:
        l3kvg::Query query_;
        std::string_view entry_node_type_;
        std::vector<std::string_view> target_node_types_;
        
        struct ReturnField {
            std::string bson_key;
            l3kvg::Query::AggOp agg = l3kvg::Query::AggOp::None;
        };
        std::vector<ReturnField> return_fields_;

        void resolve_traversals();

        std::vector<PathStep> find_path(std::string_view source, std::string_view target);
        std::string_view find_edge(std::string_view source_type, std::string_view target_type);
    };

} // namespace irods::catalog::compiler
