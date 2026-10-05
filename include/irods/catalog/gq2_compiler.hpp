#pragma once

#include <string>
#include <vector>
#include <variant>
#include <unordered_map>
#include "buffer.hpp"
#include "irods/private/genquery2_ast_types.hpp"

namespace irods::experimental::genquery2 {
    struct options;
}

namespace irods::catalog::compiler {

    struct GraphMap {
        std::string_view node_type;
        std::string_view bson_key;
    };

    extern const std::unordered_map<int, GraphMap> COLUMN_MAP;
    extern const std::unordered_map<std::string_view, GraphMap> COLUMN_NAME_MAP;

    enum class DmlAction {
        Insert,
        Update,
        Remove
    };

    struct DmlCondition {
        std::string property;
        int op = 0; // 0: =, 1: !=, 2: >, 3: >=, 4: <, 5: <=, 6: like
        std::string value;

        bool operator==(const DmlCondition& o) const {
            return property == o.property && op == o.op && value == o.value;
        }
    };

    struct DmlPlan {
        DmlAction action;
        std::string entity_type; // e.g. "DataObject", "Collection", "User", "Resource", etc.
        std::unordered_map<std::string, std::string> properties; // mapped BSON property keys, e.g. "n" -> "file.txt"
        std::vector<DmlCondition> conditions;
        std::vector<std::string> target_edges;
    };

    /**
     * Gq2ToL3kvgCompiler translates iRODS GenQuery2 AST into 
     * L3KVG Federated Query JSON.
     */
    class Gq2ToL3kvgCompiler {
    public:
        Gq2ToL3kvgCompiler() = default;
        
        lite3cpp::Buffer compile(const irods::experimental::genquery2::select& ast, std::string_view override_root_alias = "", const irods::experimental::genquery2::options* opts = nullptr);

        DmlPlan compile(const irods::experimental::genquery2::insert& ast) const;
        DmlPlan compile(const irods::experimental::genquery2::update& ast) const;
        DmlPlan compile(const irods::experimental::genquery2::remove& ast) const;
        DmlPlan compile(const irods::experimental::genquery2::statement& ast) const;

        struct PathStep {
            enum class Direction { Out, In };
            Direction dir;
            std::string_view edge_label;
            std::string_view target_type;
        };

        void add_target_type(std::string_view t) { target_node_types_.emplace_back(t); }
        const std::vector<std::string>& get_target_types() const { return target_node_types_; }
        void set_entry_type(std::string_view t) { entry_node_type_ = t; }
        const std::string& get_entry_type() const { return entry_node_type_; }

    private:
        std::string entry_node_type_;
        std::vector<std::string> target_node_types_;

        std::vector<PathStep> find_path(std::string_view source, std::string_view target);
        std::string_view find_edge(std::string_view source_type, std::string_view target_type);
    };

} // namespace irods::catalog::compiler
