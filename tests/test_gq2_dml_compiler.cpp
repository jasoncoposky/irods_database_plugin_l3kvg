#include <gtest/gtest.h>
#include "irods/catalog/gq2_compiler.hpp"
#include "irods/private/genquery2_ast_types.hpp"

namespace gq2 = irods::experimental::genquery2;
using namespace irods::catalog::compiler;

TEST(Gq2DmlCompilerTest, CompileInsertDataObject) {
    Gq2ToL3kvgCompiler compiler;

    gq2::insert insert_ast;
    insert_ast.target_entity = "DATA_NAME";
    insert_ast.assignments = {
        {"DATA_NAME", "file.txt"},
        {"COLL_NAME", "/tempZone/home/rods"}
    };

    auto plan = compiler.compile(insert_ast);
    EXPECT_EQ(plan.action, DmlAction::Insert);
    EXPECT_EQ(plan.entity_type, "DataObject");
    EXPECT_EQ(plan.properties.at("n"), "file.txt");
    EXPECT_EQ(plan.properties.at("parent_coll"), "/tempZone/home/rods");
}

TEST(Gq2DmlCompilerTest, CompileInsertCollection) {
    Gq2ToL3kvgCompiler compiler;

    gq2::insert insert_ast;
    insert_ast.target_entity = "COLLECTION";
    insert_ast.assignments = {
        {"COLL_NAME", "/tempZone/home/rods/sub"}
    };

    auto plan = compiler.compile(insert_ast);
    EXPECT_EQ(plan.action, DmlAction::Insert);
    EXPECT_EQ(plan.entity_type, "Collection");
    EXPECT_EQ(plan.properties.at("n"), "/tempZone/home/rods/sub");
}

TEST(Gq2DmlCompilerTest, CompileUpdateDataObject) {
    Gq2ToL3kvgCompiler compiler;

    gq2::update update_ast;
    update_ast.target_entity = "DataObject";
    update_ast.assignments = {
        {"DATA_SIZE", "1024"},
        {"DATA_CHECKSUM", "sha2:xyz"}
    };
    update_ast.where_conditions.push_back(
        gq2::condition{gq2::column{"DATA_NAME"}, gq2::condition_equal{"file.txt"}}
    );

    auto plan = compiler.compile(update_ast);
    EXPECT_EQ(plan.action, DmlAction::Update);
    EXPECT_EQ(plan.entity_type, "DataObject");
    EXPECT_EQ(plan.properties.at("s"), "1024");
    EXPECT_EQ(plan.properties.at("cs"), "sha2:xyz");
    ASSERT_EQ(plan.conditions.size(), 1u);
    EXPECT_EQ(plan.conditions[0].first, "n");
    EXPECT_EQ(plan.conditions[0].second, "file.txt");
}

TEST(Gq2DmlCompilerTest, CompileRemoveDataObject) {
    Gq2ToL3kvgCompiler compiler;

    gq2::remove remove_ast;
    remove_ast.target_entity = "DataObject";
    remove_ast.where_conditions.push_back(
        gq2::condition{gq2::column{"DATA_NAME"}, gq2::condition_equal{"file.txt"}}
    );

    auto plan = compiler.compile(remove_ast);
    EXPECT_EQ(plan.action, DmlAction::Remove);
    EXPECT_EQ(plan.entity_type, "DataObject");
    ASSERT_EQ(plan.conditions.size(), 1u);
    EXPECT_EQ(plan.conditions[0].first, "n");
    EXPECT_EQ(plan.conditions[0].second, "file.txt");
}

TEST(Gq2DmlCompilerTest, CompileStatementVariant) {
    Gq2ToL3kvgCompiler compiler;

    gq2::insert insert_ast;
    insert_ast.target_entity = "DataObject";
    insert_ast.assignments = {{"DATA_NAME", "file.txt"}};
    gq2::statement stmt_insert = insert_ast;
    auto plan1 = compiler.compile(stmt_insert);
    EXPECT_EQ(plan1.action, DmlAction::Insert);
    EXPECT_EQ(plan1.entity_type, "DataObject");
    EXPECT_EQ(plan1.properties.at("n"), "file.txt");

    gq2::update update_ast;
    update_ast.target_entity = "DataObject";
    update_ast.assignments = {{"DATA_SIZE", "2048"}};
    update_ast.where_conditions.push_back(
        gq2::condition{gq2::column{"DATA_NAME"}, gq2::condition_equal{"file.txt"}}
    );
    gq2::statement stmt_update = update_ast;
    auto plan2 = compiler.compile(stmt_update);
    EXPECT_EQ(plan2.action, DmlAction::Update);
    EXPECT_EQ(plan2.properties.at("s"), "2048");

    gq2::remove remove_ast;
    remove_ast.target_entity = "DataObject";
    remove_ast.where_conditions.push_back(
        gq2::condition{gq2::column{"DATA_NAME"}, gq2::condition_equal{"file.txt"}}
    );
    gq2::statement stmt_remove = remove_ast;
    auto plan3 = compiler.compile(stmt_remove);
    EXPECT_EQ(plan3.action, DmlAction::Remove);

    gq2::statement stmt_select = gq2::select{};
    EXPECT_THROW(compiler.compile(stmt_select), std::invalid_argument);
}

TEST(Gq2DmlCompilerTest, InferEntityFromColumnsAndConditions) {
    Gq2ToL3kvgCompiler compiler;

    // Infer from insert assignments
    gq2::insert insert_ast;
    insert_ast.target_entity = "";
    insert_ast.assignments = {{"USER_NAME", "alice"}};
    auto plan1 = compiler.compile(insert_ast);
    EXPECT_EQ(plan1.entity_type, "User");
    EXPECT_EQ(plan1.properties.at("n"), "alice");

    // Infer from remove condition
    gq2::remove remove_ast;
    remove_ast.target_entity = "";
    remove_ast.where_conditions.push_back(
        gq2::condition{gq2::column{"COLL_NAME"}, gq2::condition_equal{"/tempZone/home"}}
    );
    auto plan2 = compiler.compile(remove_ast);
    EXPECT_EQ(plan2.entity_type, "Collection");
    ASSERT_EQ(plan2.conditions.size(), 1u);
    EXPECT_EQ(plan2.conditions[0].first, "n");
    EXPECT_EQ(plan2.conditions[0].second, "/tempZone/home");
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
