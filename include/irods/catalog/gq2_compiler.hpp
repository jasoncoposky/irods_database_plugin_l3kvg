#pragma once

#include <string>
#include <vector>
#include <variant>
#include <unordered_map>
#include "irods/private/genquery2_ast_types.hpp"

namespace irods::catalog::compiler {

    struct GraphMap {
        std::string_view node_type;
        std::string_view bson_key;
    };

    extern const std::unordered_map<int, GraphMap> COLUMN_MAP;
    extern const std::unordered_map<std::string_view, GraphMap> COLUMN_NAME_MAP;

    /**
     * Gq2ToL3kvgCompiler translates iRODS GenQuery2 AST into 
     * L3KVG Federated Query JSON.
     */
    class Gq2ToL3kvgCompiler {
    public:
        Gq2ToL3kvgCompiler() = default;
        
        std::string compile(const irods::experimental::genquery2::select& ast);

        struct PathStep {
            enum class Direction { Out, In };
            Direction dir;
            std::string_view edge_label;
            std::string_view target_type;
        };

        void add_target_type(std::string_view t) { target_node_types_.push_back(t); }
        void set_entry_type(std::string_view t) { entry_node_type_ = t; }

    private:
        std::string_view entry_node_type_;
        std::vector<std::string_view> target_node_types_;

        std::vector<PathStep> find_path(std::string_view source, std::string_view target);
        std::string_view find_edge(std::string_view source_type, std::string_view target_type);
    };

} // namespace irods::catalog::compiler
