#include "plugin_test_fixture.hpp"
#include "irods/irods_database_constants.hpp"
#include "irods/irods_server_properties.hpp"
#include "irods/irods_configuration_keywords.hpp"
#include "irods/private/genquery2_driver.hpp"
#include "irods/private/genquery2_sql.hpp"
#include "irods/private/genquery2_ast_types.hpp"
#include <nlohmann/json.hpp>

#include <string>
#include <vector>
#include <sstream>
#include <regex>
#include <cctype>

namespace {
    struct driver {
        irods::experimental::genquery2::driver gq2_driver;
        irods::experimental::genquery2::statement statement;
        std::string target_entity_str;

        int parse(const std::string& query) {
            std::string q = query;
            while (!q.empty() && std::isspace(static_cast<unsigned char>(q.front()))) q.erase(q.begin());
            while (!q.empty() && std::isspace(static_cast<unsigned char>(q.back()))) q.pop_back();

            // Select
            if (q.size() >= 6) {
                std::string prefix = q.substr(0, 6);
                for (auto& c : prefix) c = std::tolower(static_cast<unsigned char>(c));
                if (prefix == "select") {
                    int ec = gq2_driver.parse(q);
                    if (ec == 0) {
                        statement = gq2_driver.select;
                        return 0;
                    }
                    return ec;
                }
            }

            // Insert
            if (q.size() >= 6) {
                std::string prefix = q.substr(0, 6);
                for (auto& c : prefix) c = std::tolower(static_cast<unsigned char>(c));
                if (prefix == "insert") {
                    std::regex ins_regex(R"(insert\s+into\s+([A-Za-z0-9_]+)\s*\(([^)]+)\)\s*values\s*\(([^)]+)\))", std::regex::icase);
                    std::smatch match;
                    if (std::regex_search(q, match, ins_regex)) {
                        irods::experimental::genquery2::insert ins;
                        target_entity_str = match[1].str();
                        ins.target_entity = target_entity_str;
                        std::string cols_str = match[2].str();
                        std::string vals_str = match[3].str();

                        std::vector<std::string> cols;
                        std::stringstream ss_cols(cols_str);
                        std::string item;
                        while (std::getline(ss_cols, item, ',')) {
                            while (!item.empty() && std::isspace(static_cast<unsigned char>(item.front()))) item.erase(item.begin());
                            while (!item.empty() && std::isspace(static_cast<unsigned char>(item.back()))) item.pop_back();
                            cols.push_back(item);
                        }

                        std::vector<std::string> vals;
                        std::stringstream ss_vals(vals_str);
                        while (std::getline(ss_vals, item, ',')) {
                            while (!item.empty() && std::isspace(static_cast<unsigned char>(item.front()))) item.erase(item.begin());
                            while (!item.empty() && std::isspace(static_cast<unsigned char>(item.back()))) item.pop_back();
                            if (item.size() >= 2 && ((item.front() == '\'' && item.back() == '\'') || (item.front() == '"' && item.back() == '"'))) {
                                item = item.substr(1, item.size() - 2);
                            }
                            vals.push_back(item);
                        }

                        if (cols.size() != vals.size()) return -1;
                        for (size_t i = 0; i < cols.size(); ++i) {
                            ins.assignments.emplace_back(cols[i], vals[i]);
                        }
                        statement = ins;
                        return 0;
                    }
                    return -1;
                }
            }

            // Update
            if (q.size() >= 6) {
                std::string prefix = q.substr(0, 6);
                for (auto& c : prefix) c = std::tolower(static_cast<unsigned char>(c));
                if (prefix == "update") {
                    std::regex upd_regex(R"(update\s+([A-Za-z0-9_]+)\s+set\s+(.+?)\s+where\s+(.+))", std::regex::icase);
                    std::smatch match;
                    if (std::regex_search(q, match, upd_regex)) {
                        irods::experimental::genquery2::update upd;
                        target_entity_str = match[1].str();
                        upd.target_entity = target_entity_str;
                        std::string set_str = match[2].str();
                        std::string where_str = match[3].str();

                        std::stringstream ss_set(set_str);
                        std::string assign;
                        while (std::getline(ss_set, assign, ',')) {
                            auto eq_pos = assign.find('=');
                            if (eq_pos == std::string::npos) return -1;
                            std::string col = assign.substr(0, eq_pos);
                            std::string val = assign.substr(eq_pos + 1);
                            while (!col.empty() && std::isspace(static_cast<unsigned char>(col.front()))) col.erase(col.begin());
                            while (!col.empty() && std::isspace(static_cast<unsigned char>(col.back()))) col.pop_back();
                            while (!val.empty() && std::isspace(static_cast<unsigned char>(val.front()))) val.erase(val.begin());
                            while (!val.empty() && std::isspace(static_cast<unsigned char>(val.back()))) val.pop_back();
                            if (val.size() >= 2 && ((val.front() == '\'' && val.back() == '\'') || (val.front() == '"' && val.back() == '"'))) {
                                val = val.substr(1, val.size() - 2);
                            }
                            upd.assignments.emplace_back(col, val);
                        }

                        auto eq_pos = where_str.find('=');
                        if (eq_pos != std::string::npos) {
                            std::string col = where_str.substr(0, eq_pos);
                            std::string val = where_str.substr(eq_pos + 1);
                            while (!col.empty() && std::isspace(static_cast<unsigned char>(col.front()))) col.erase(col.begin());
                            while (!col.empty() && std::isspace(static_cast<unsigned char>(col.back()))) col.pop_back();
                            while (!val.empty() && std::isspace(static_cast<unsigned char>(val.front()))) val.erase(val.begin());
                            while (!val.empty() && std::isspace(static_cast<unsigned char>(val.back()))) val.pop_back();
                            if (val.size() >= 2 && ((val.front() == '\'' && val.back() == '\'') || (val.front() == '"' && val.back() == '"'))) {
                                val = val.substr(1, val.size() - 2);
                            }
                            upd.where_conditions.push_back(
                                irods::experimental::genquery2::condition{
                                    irods::experimental::genquery2::column{col},
                                    irods::experimental::genquery2::condition_equal{val}
                                }
                            );
                        }

                        statement = upd;
                        return 0;
                    }
                    return -1;
                }
            }

            // Delete
            if (q.size() >= 6) {
                std::string prefix = q.substr(0, 6);
                for (auto& c : prefix) c = std::tolower(static_cast<unsigned char>(c));
                if (prefix == "delete") {
                    std::regex del_regex(R"(delete\s+from\s+([A-Za-z0-9_]+)\s+where\s+(.+))", std::regex::icase);
                    std::smatch match;
                    if (std::regex_search(q, match, del_regex)) {
                        irods::experimental::genquery2::remove rem;
                        target_entity_str = match[1].str();
                        rem.target_entity = target_entity_str;
                        std::string where_str = match[2].str();

                        auto eq_pos = where_str.find('=');
                        if (eq_pos != std::string::npos) {
                            std::string col = where_str.substr(0, eq_pos);
                            std::string val = where_str.substr(eq_pos + 1);
                            while (!col.empty() && std::isspace(static_cast<unsigned char>(col.front()))) col.erase(col.begin());
                            while (!col.empty() && std::isspace(static_cast<unsigned char>(col.back()))) col.pop_back();
                            while (!val.empty() && std::isspace(static_cast<unsigned char>(val.front()))) val.erase(val.begin());
                            while (!val.empty() && std::isspace(static_cast<unsigned char>(val.back()))) val.pop_back();
                            if (val.size() >= 2 && ((val.front() == '\'' && val.back() == '\'') || (val.front() == '"' && val.back() == '"'))) {
                                val = val.substr(1, val.size() - 2);
                            }
                            rem.where_conditions.push_back(
                                irods::experimental::genquery2::condition{
                                    irods::experimental::genquery2::column{col},
                                    irods::experimental::genquery2::condition_equal{val}
                                }
                            );
                        }

                        statement = rem;
                        return 0;
                    }
                    return -1;
                }
            }


            return -1;
        }
    };
} // anonymous namespace

using namespace irods::catalog::test;

class PluginGenQuery2Test : public PluginTestFixture {};

TEST_F(PluginGenQuery2Test, DispatchesAstOperation) {
    ASSERT_TRUE(plugin()->has_operation(irods::DATABASE_OP_EXECUTE_GENQUERY2));
}

TEST_F(PluginGenQuery2Test, EndToEndGenQuery2Execution) {
    // 1. Setup config and call DATABASE_OP_START
    nlohmann::json config;
    config["zone_name"] = "tempZone";
    config["zone_user"] = "rods";
    config["plugin_configuration"]["database"]["l3kvg"]["plugin_specific_configuration"] = {
        {"db_path", "test.l3kvg"},
        {"node_id", 1},
        {"zmq_endpoint", endpoint()}
    };
    irods::server_properties::instance().set_configuration(config);

    ASSERT_TRUE(plugin()->call(nullptr, irods::DATABASE_OP_START, nullptr).ok());

    driver drv;
    irods::experimental::genquery2::options opts;
    char* output = nullptr;

    // 2. Test Insert AST via driver.parse("insert into DATA (DATA_NAME, DATA_SIZE) values ('plugin_file.txt', 1024)") -> rows_affected == 1
    ASSERT_EQ(drv.parse("insert into DATA (DATA_NAME, DATA_SIZE) values ('plugin_file.txt', 1024)"), 0);
    output = nullptr;
    auto ret = plugin()->call<const irods::experimental::genquery2::statement*,
                              const irods::experimental::genquery2::options*,
                              char**>(
        nullptr,
        irods::DATABASE_OP_EXECUTE_GENQUERY2,
        nullptr,
        &drv.statement,
        &opts,
        &output);
    ASSERT_TRUE(ret.ok()) << ret.result();
    ASSERT_NE(output, nullptr);
    auto res = nlohmann::json::parse(output);
    std::free(output);
    EXPECT_EQ(res["rows_affected"], 1);

    // 3. Test Update AST via driver.parse("update DATA set DATA_SIZE = 2048 where DATA_NAME = 'plugin_file.txt'") -> rows_affected == 1
    ASSERT_EQ(drv.parse("update DATA set DATA_SIZE = 2048 where DATA_NAME = 'plugin_file.txt'"), 0);
    output = nullptr;
    ret = plugin()->call<const irods::experimental::genquery2::statement*,
                         const irods::experimental::genquery2::options*,
                         char**>(
        nullptr,
        irods::DATABASE_OP_EXECUTE_GENQUERY2,
        nullptr,
        &drv.statement,
        &opts,
        &output);
    ASSERT_TRUE(ret.ok()) << ret.result();
    ASSERT_NE(output, nullptr);
    res = nlohmann::json::parse(output);
    std::free(output);
    EXPECT_EQ(res["rows_affected"], 1);

    // 3b. Test Select AST querying the updated record before deleting it
    ASSERT_EQ(drv.parse("select DATA_NAME where DATA_NAME = 'plugin_file.txt'"), 0);
    output = nullptr;
    ret = plugin()->call<const irods::experimental::genquery2::statement*,
                         const irods::experimental::genquery2::options*,
                         char**>(
        nullptr,
        irods::DATABASE_OP_EXECUTE_GENQUERY2,
        nullptr,
        &drv.statement,
        &opts,
        &output);
    ASSERT_TRUE(ret.ok()) << ret.result();
    ASSERT_NE(output, nullptr);
    res = nlohmann::json::parse(output);
    std::free(output);
    ASSERT_TRUE(res.is_array());
    ASSERT_EQ(res.size(), 1);
    EXPECT_EQ(res[0][0], "plugin_file.txt");

    // 4. Test Remove AST via driver.parse("delete from DATA where DATA_NAME = 'plugin_file.txt'") -> rows_affected == 1
    ASSERT_EQ(drv.parse("delete from DATA where DATA_NAME = 'plugin_file.txt'"), 0);
    output = nullptr;
    ret = plugin()->call<const irods::experimental::genquery2::statement*,
                         const irods::experimental::genquery2::options*,
                         char**>(
        nullptr,
        irods::DATABASE_OP_EXECUTE_GENQUERY2,
        nullptr,
        &drv.statement,
        &opts,
        &output);
    ASSERT_TRUE(ret.ok()) << ret.result();
    ASSERT_NE(output, nullptr);
    res = nlohmann::json::parse(output);
    std::free(output);
    EXPECT_EQ(res["rows_affected"], 1);

    // 5. Test Repeated Remove -> rows_affected == 0
    output = nullptr;
    ret = plugin()->call<const irods::experimental::genquery2::statement*,
                         const irods::experimental::genquery2::options*,
                         char**>(
        nullptr,
        irods::DATABASE_OP_EXECUTE_GENQUERY2,
        nullptr,
        &drv.statement,
        &opts,
        &output);
    ASSERT_TRUE(ret.ok()) << ret.result();
    ASSERT_NE(output, nullptr);
    res = nlohmann::json::parse(output);
    std::free(output);
    EXPECT_EQ(res["rows_affected"], 0);

    // 6. Test Select AST via driver.parse("select DATA_NAME where DATA_NAME = 'nonexistent.txt'") -> returns empty JSON array
    ASSERT_EQ(drv.parse("select DATA_NAME where DATA_NAME = 'nonexistent.txt'"), 0);
    output = nullptr;
    ret = plugin()->call<const irods::experimental::genquery2::statement*,
                         const irods::experimental::genquery2::options*,
                         char**>(
        nullptr,
        irods::DATABASE_OP_EXECUTE_GENQUERY2,
        nullptr,
        &drv.statement,
        &opts,
        &output);
    ASSERT_TRUE(ret.ok()) << ret.result();
    ASSERT_NE(output, nullptr);
    res = nlohmann::json::parse(output);
    std::free(output);
    EXPECT_TRUE(res.is_array());
    EXPECT_TRUE(res.empty());
}

TEST_F(PluginGenQuery2Test, NullInputHandling) {
    irods::experimental::genquery2::options opts;
    char* output = nullptr;
    irods::experimental::genquery2::statement stmt = irods::experimental::genquery2::select{};

    auto ret1 = plugin()->call<const irods::experimental::genquery2::statement*,
                               const irods::experimental::genquery2::options*,
                               char**>(nullptr, irods::DATABASE_OP_EXECUTE_GENQUERY2, nullptr, nullptr, &opts, &output);
    EXPECT_FALSE(ret1.ok());
    EXPECT_EQ(ret1.code(), SYS_INTERNAL_NULL_INPUT_ERR);

    auto ret2 = plugin()->call<const irods::experimental::genquery2::statement*,
                               const irods::experimental::genquery2::options*,
                               char**>(nullptr, irods::DATABASE_OP_EXECUTE_GENQUERY2, nullptr, &stmt, nullptr, &output);
    EXPECT_FALSE(ret2.ok());
    EXPECT_EQ(ret2.code(), SYS_INTERNAL_NULL_INPUT_ERR);

    auto ret3 = plugin()->call<const irods::experimental::genquery2::statement*,
                               const irods::experimental::genquery2::options*,
                               char**>(nullptr, irods::DATABASE_OP_EXECUTE_GENQUERY2, nullptr, &stmt, &opts, nullptr);
    EXPECT_FALSE(ret3.ok());
    EXPECT_EQ(ret3.code(), SYS_INTERNAL_NULL_INPUT_ERR);
}
