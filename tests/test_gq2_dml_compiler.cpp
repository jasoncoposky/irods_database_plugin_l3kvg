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
    EXPECT_EQ(plan.conditions[0].property, "n");
    EXPECT_EQ(plan.conditions[0].value, "file.txt");
    EXPECT_EQ(plan.conditions[0].op, 0);
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
    EXPECT_EQ(plan.conditions[0].property, "n");
    EXPECT_EQ(plan.conditions[0].value, "file.txt");
    EXPECT_EQ(plan.conditions[0].op, 0);
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

    // Priority inference: COLL_NAME + DATA_NAME -> DataObject (prio 4 > prio 3)
    gq2::insert insert_prio;
    insert_prio.target_entity = "";
    insert_prio.assignments = {{"COLL_NAME", "/tempZone/home"}, {"DATA_NAME", "data.dat"}};
    auto plan_prio = compiler.compile(insert_prio);
    EXPECT_EQ(plan_prio.entity_type, "DataObject");
    EXPECT_EQ(plan_prio.properties.at("parent_coll"), "/tempZone/home");
    EXPECT_EQ(plan_prio.properties.at("n"), "data.dat");

    // Infer from remove condition
    gq2::remove remove_ast;
    remove_ast.target_entity = "";
    remove_ast.where_conditions.push_back(
        gq2::condition{gq2::column{"COLL_NAME"}, gq2::condition_equal{"/tempZone/home"}}
    );
    auto plan2 = compiler.compile(remove_ast);
    EXPECT_EQ(plan2.entity_type, "Collection");
    ASSERT_EQ(plan2.conditions.size(), 1u);
    EXPECT_EQ(plan2.conditions[0].property, "n");
    EXPECT_EQ(plan2.conditions[0].value, "/tempZone/home");
}

TEST(Gq2DmlCompilerTest, CompoundConditionsAndOperators) {
    Gq2ToL3kvgCompiler compiler;

    gq2::update update_ast;
    update_ast.target_entity = "DataObject";
    update_ast.assignments = {{"DATA_MODIFY_TIME", "1700000000"}};

    // Compound AND condition: DATA_NAME = "foo" AND DATA_SIZE > "500"
    gq2::logical_and and_node;
    and_node.condition.push_back(gq2::condition{gq2::column{"DATA_NAME"}, gq2::condition_equal{"foo"}});
    and_node.condition.push_back(gq2::condition{gq2::column{"DATA_SIZE"}, gq2::condition_greater_than{"500"}});
    update_ast.where_conditions.push_back(and_node);

    auto plan = compiler.compile(update_ast);
    ASSERT_EQ(plan.conditions.size(), 2u);
    EXPECT_EQ(plan.conditions[0].property, "n");
    EXPECT_EQ(plan.conditions[0].value, "foo");
    EXPECT_EQ(plan.conditions[0].op, 0);

    EXPECT_EQ(plan.conditions[1].property, "s");
    EXPECT_EQ(plan.conditions[1].value, "500");
    EXPECT_EQ(plan.conditions[1].op, 2); // greater than
}

TEST(Gq2DmlCompilerTest, RejectUnsupportedLogicalOperators) {
    Gq2ToL3kvgCompiler compiler;

    // Reject OR in DML
    gq2::remove remove_or;
    remove_or.target_entity = "DataObject";
    gq2::logical_or or_node;
    or_node.condition.push_back(gq2::condition{gq2::column{"DATA_NAME"}, gq2::condition_equal{"foo"}});
    or_node.condition.push_back(gq2::condition{gq2::column{"DATA_NAME"}, gq2::condition_equal{"bar"}});
    remove_or.where_conditions.push_back(or_node);
    EXPECT_THROW(compiler.compile(remove_or), std::invalid_argument);

    // Reject NOT in DML
    gq2::remove remove_not;
    remove_not.target_entity = "DataObject";
    gq2::logical_not not_node;
    not_node.condition.push_back(gq2::condition{gq2::column{"DATA_NAME"}, gq2::condition_equal{"foo"}});
    remove_not.where_conditions.push_back(not_node);
    EXPECT_THROW(compiler.compile(remove_not), std::invalid_argument);
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
