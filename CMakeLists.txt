cmake_minimum_required(VERSION 3.20)
project(irods_database_plugin_l3kvg VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

add_compile_options(-Wno-changes-meaning)

# find_package(GTest REQUIRED) # Use internal build

# iRODS Dependencies
# In a real environment, we'd find_package(IRODS)
# For this prototype, we'll set up the include paths manually based on the research.
set(IRODS_INCLUDE_DIRS
    ${CMAKE_CURRENT_SOURCE_DIR}/../irods_dev/irods/lib/core/include
    ${CMAKE_CURRENT_SOURCE_DIR}/../irods_dev/irods/lib/api/include
    ${CMAKE_CURRENT_SOURCE_DIR}/../irods_dev/irods/server/core/include
    ${CMAKE_CURRENT_SOURCE_DIR}/../irods_dev/irods/server/icat/include
    ${CMAKE_CURRENT_SOURCE_DIR}/../irods_dev/irods/server/re/include
    ${CMAKE_CURRENT_SOURCE_DIR}/../irods_dev/irods/server/genquery2/include
    ${CMAKE_CURRENT_SOURCE_DIR}/../irods_dev/irods_build_output_dir/server/genquery2/dsl
)

# L3KVG Dependencies
set(L3KVG_INCLUDE_DIRS
    ${CMAKE_CURRENT_SOURCE_DIR}/../l3kvg/include
    ${CMAKE_CURRENT_SOURCE_DIR}/../L3KV/src
    ${CMAKE_CURRENT_SOURCE_DIR}/../lib/lite3-cpp/include
    ${CMAKE_CURRENT_SOURCE_DIR}/../libconveyor/include
    ${CMAKE_CURRENT_SOURCE_DIR}/../lib/liblite3client/external
    ${CMAKE_CURRENT_SOURCE_DIR}/../lib/boost_1_89_0
)

include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${IRODS_INCLUDE_DIRS}
    ${L3KVG_INCLUDE_DIRS}
)

# Build dependencies
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/../l3kvg ${CMAKE_CURRENT_BINARY_DIR}/l3kvg)

add_library(l3kvg_plugin SHARED
    src/db_plugin.cpp
    src/catalog/catalog_facade.cpp
    src/compiler/gq2_compiler.cpp
)

target_link_libraries(l3kvg_plugin PRIVATE
    l3kvg_engine
    l3kv_engine
    lite3-cpp
    conveyor
)

target_compile_options(l3kvg_plugin PRIVATE
    -Wno-changes-meaning
)

set_target_properties(l3kvg_plugin PROPERTIES
    PREFIX ""
    OUTPUT_NAME "libirods_database_plugin_l3kvg"
)

# Link dependencies for iRODS (found in build output)
link_directories(/home/darkfell/dev/irods_dev/irods_build_output_dir/lib)
link_directories(/home/darkfell/dev/irods_dev/irods_build_output_dir/server)

# Tests
add_executable(test_metadata_acls tests/test_metadata_acls.cpp)
target_link_libraries(test_metadata_acls PRIVATE
    l3kvg_plugin
    l3kvg_engine
    l3kv_engine
    lite3-cpp
    conveyor
    gtest
    gtest_main
    irods_common
    irods_server
)

add_executable(test_identity tests/test_identity.cpp)
target_link_libraries(test_identity PRIVATE
    l3kvg_plugin
    l3kvg_engine
    l3kv_engine
    lite3-cpp
    conveyor
    gtest
    gtest_main
    irods_common
    irods_server
)

add_executable(test_collections tests/test_collections.cpp)
target_link_libraries(test_collections PRIVATE
    l3kvg_plugin
    l3kvg_engine
    l3kv_engine
    lite3-cpp
    conveyor
    gtest
    gtest_main
    irods_common
    irods_server
)

add_executable(test_gq2_metadata_compiler tests/test_gq2_metadata_compiler.cpp)
target_link_libraries(test_gq2_metadata_compiler PRIVATE
    l3kvg_plugin
    l3kvg_engine
    l3kv_engine
    lite3-cpp
    conveyor
    gtest
    gtest_main
    irods_common
    irods_server
)

add_executable(test_multihop_query tests/test_multihop_query.cpp)
target_link_libraries(test_multihop_query PRIVATE
    l3kvg_plugin
    l3kvg_engine
    l3kv_engine
    lite3-cpp
    conveyor
    gtest
    gtest_main
    irods_common
    irods_server
)

add_executable(test_advanced_operators tests/test_advanced_operators.cpp)
target_link_libraries(test_advanced_operators PRIVATE
    l3kvg_plugin
    l3kvg_engine
    l3kv_engine
    lite3-cpp
    conveyor
    gtest
    gtest_main
    irods_common
    irods_server
)

add_executable(test_nested_logic tests/test_nested_logic.cpp)
target_link_libraries(test_nested_logic PRIVATE
    l3kvg_plugin
    l3kvg_engine
    l3kv_engine
    lite3-cpp
    conveyor
    gtest
    gtest_main
    irods_common
    irods_server
)

add_executable(test_aggregates tests/test_aggregates.cpp)
target_link_libraries(test_aggregates PRIVATE
    l3kvg_plugin
    l3kvg_engine
    l3kv_engine
    lite3-cpp
    conveyor
    gtest
    gtest_main
    irods_common
    irods_server
)

add_executable(test_sorting tests/test_sorting.cpp)
target_link_libraries(test_sorting PRIVATE
    l3kvg_plugin
    l3kvg_engine
    l3kv_engine
    lite3-cpp
    conveyor
    gtest
    gtest_main
    irods_common
    irods_server
)

add_executable(test_deep_traversal tests/test_deep_traversal.cpp)
target_link_libraries(test_deep_traversal PRIVATE
    l3kvg_plugin
    l3kvg_engine
    l3kv_engine
    lite3-cpp
    conveyor
    gtest
    gtest_main
    irods_common
    irods_server
)

add_executable(test_groupby tests/test_groupby.cpp)
target_link_libraries(test_groupby PRIVATE
    l3kvg_plugin
    l3kvg_engine
    l3kv_engine
    lite3-cpp
    conveyor
    gtest
    gtest_main
    irods_common
    irods_server
)

add_executable(test_distinct tests/test_distinct.cpp)
target_link_libraries(test_distinct PRIVATE
    l3kvg_plugin
    l3kvg_engine
    l3kv_engine
    lite3-cpp
    conveyor
    gtest
    gtest_main
    irods_common
    irods_server
)
