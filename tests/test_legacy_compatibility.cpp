#include <gtest/gtest.h>
#include "irods/catalog/catalog_facade.hpp"
#include "irods/catalog/gq2_compiler.hpp"
#include "irods/private/genquery2_driver.hpp"
#include "L3KVG/Engine.hpp"
#include "engine/store.hpp"

#include <iostream>
#include <fstream>
#include <string>
#include <vector>

using namespace irods::catalog;

TEST(LegacyCompatibilityTest, CompileAllLegacyQueries) {
    l3kvg::Engine engine("legacy.l3kvg", 1);
    compiler::Gq2ToL3kvgCompiler compiler;

    std::ifstream file("legacy_queries.txt");
    ASSERT_TRUE(file.is_open()) << "Could not open legacy_queries.txt. Ensure it was downloaded successfully.";

    std::string line;
    int count = 0;
    int success_count = 0;
    int parse_fail_count = 0;
    int compile_fail_count = 0;
    
    while (std::getline(file, line)) {
        if (line.empty() || line[0] == '#') continue;
        count++;

        try {
            irods::experimental::genquery2::driver drv;
            if (drv.parse(line) != 0) {
                parse_fail_count++;
                continue;
            }

            try {
                auto query = compiler.compile(drv.select);
                success_count++;
            } catch (const std::exception& e) {
                compile_fail_count++;
            }
        } catch (...) {
            parse_fail_count++;
        }
    }

    std::cout << "[LegacyCompatibility] Summary:\n";
    std::cout << "  Total Queries:      " << count << "\n";
    std::cout << "  Parsed Successfully: " << (count - parse_fail_count) << "\n";
    std::cout << "  Compiled Successfully: " << success_count << "\n";
    std::cout << "  Parse Failures:     " << parse_fail_count << " (Likely legacy || syntax)\n";
    std::cout << "  Compile Failures:   " << compile_fail_count << "\n";

    // We expect zero compile failures for queries that parsed successfully
    EXPECT_EQ(compile_fail_count, 0);
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
