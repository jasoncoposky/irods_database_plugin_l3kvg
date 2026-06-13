#include <gtest/gtest.h>
#include <string>

// We'll just copy the logic here to test it in isolation
static bool is_numeric_test(const std::string& name) {
    return (name == "DATA_ID" || name == "COLL_ID" || name == "USER_ID" || 
            name == "RESC_ID" || name == "ZONE_ID" || name == "DATA_SIZE" || 
            name == "DATA_REPL_NUM" || name == "DATA_REPL_STATUS" ||
            name == "RESC_FREE_SPACE" || name == "USER_PRIORITY" ||
            name == "DATA_EXPIRY" || name == "DATA_MAP_ID" ||
            name == "COLL_MAP_ID" || name.find("TIME") != std::string::npos);
}

TEST(BridgeTest, IsNumericCoversAllIds) {
    EXPECT_TRUE(is_numeric_test("DATA_ID"));
    EXPECT_TRUE(is_numeric_test("COLL_ID"));
    EXPECT_TRUE(is_numeric_test("USER_ID"));
    
    // These are currently missing and likely causing the "stat error"
    EXPECT_TRUE(is_numeric_test("DATA_COLL_ID"));
    EXPECT_TRUE(is_numeric_test("D_RESC_ID"));
    EXPECT_TRUE(is_numeric_test("USER_GROUP_ID"));
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
