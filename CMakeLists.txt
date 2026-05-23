cmake_minimum_required(VERSION 3.20)
project(irods_database_plugin_l3kvg VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

add_compile_options(-Wno-changes-meaning)

# Include Directories
include_directories(include)

# Check for iRODS headers in system paths first (for Docker builds)
find_path(IRODS_EXTERNALS_PATH NAMES irods_externals PATHS /opt/irods-externals)
if(EXISTS "/usr/include/irods")
    include_directories(/usr/include/irods)
endif()

# Fallback/Override for local development
include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}/../l3kvg/include
    ${CMAKE_CURRENT_SOURCE_DIR}/../L3KV/src
    ${CMAKE_CURRENT_SOURCE_DIR}/../L3KV/src/engine
    ${CMAKE_CURRENT_SOURCE_DIR}/../lib/lite3-cpp/include
    ${CMAKE_CURRENT_SOURCE_DIR}/../yyjson/src
    ${CMAKE_CURRENT_SOURCE_DIR}/../irods_dev/irods/server/core/include
    ${CMAKE_CURRENT_SOURCE_DIR}/../irods_dev/irods/lib/core/include
    ${CMAKE_CURRENT_SOURCE_DIR}/../irods_dev/irods/lib/api/include
    ${CMAKE_CURRENT_SOURCE_DIR}/../irods_dev/irods/server/genquery2/include
    ${CMAKE_CURRENT_SOURCE_DIR}/../irods_dev/irods_build_output_dir/server/genquery2/dsl
)

# External Libraries
link_directories(
    /usr/lib/irods/plugins/database
    ${CMAKE_CURRENT_SOURCE_DIR}/../irods_dev/irods_build_output_dir/lib
)

# Subprojects
add_subdirectory(../l3kvg l3kvg)

# Core Plugin Library
add_library(irods_database_plugin_l3kvg SHARED
    src/db_plugin.cpp
    src/catalog/catalog_facade.cpp
    src/compiler/gq2_compiler.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/../irods_dev/irods/server/genquery2/src/genquery2_driver.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/../irods_dev/irods_build_output_dir/server/genquery2/dsl/lexer.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/../irods_dev/irods_build_output_dir/server/genquery2/dsl/parser.cpp
)

set_target_properties(irods_database_plugin_l3kvg PROPERTIES
    PREFIX "lib"
    OUTPUT_NAME "l3kvg"
)

target_link_libraries(irods_database_plugin_l3kvg PRIVATE
    l3kvg_engine
    l3kv_engine
    lite3-cpp
    conveyor
    irods_common
    irods_server
    fmt
)

# Tests
macro(add_plugin_test name source)
    add_executable(${name} ${source})
    target_link_libraries(${name} PRIVATE
        irods_database_plugin_l3kvg
        l3kvg_engine
        l3kv_engine
        lite3-cpp
        conveyor
        gtest
        gtest_main
        irods_common
        irods_server
        dl
        libzmq-static
    )
endmacro()

add_plugin_test(test_plugin_identity tests/test_plugin_identity.cpp)
add_plugin_test(test_plugin_data tests/test_plugin_data.cpp)
add_plugin_test(test_plugin_acls tests/test_plugin_acls.cpp)
add_plugin_test(test_plugin_metadata tests/test_plugin_metadata.cpp)
add_plugin_test(test_plugin_federation tests/test_plugin_federation.cpp)
add_plugin_test(test_plugin_resources tests/test_plugin_resources.cpp)
add_plugin_test(test_plugin_collections tests/test_plugin_collections.cpp)
add_plugin_test(test_plugin_misc tests/test_plugin_misc.cpp)

# Logic Tests (Direct Facade/Compiler)
add_executable(test_metadata_acls tests/test_metadata_acls.cpp)
target_link_libraries(test_metadata_acls PRIVATE irods_database_plugin_l3kvg l3kvg_engine l3kv_engine lite3-cpp conveyor gtest gtest_main irods_common irods_server dl libzmq-static)

add_executable(test_collections tests/test_collections.cpp)
target_link_libraries(test_collections PRIVATE irods_database_plugin_l3kvg l3kvg_engine l3kv_engine lite3-cpp conveyor gtest gtest_main irods_common irods_server dl libzmq-static)

add_executable(test_gq2_metadata_compiler tests/test_gq2_metadata_compiler.cpp)
target_link_libraries(test_gq2_metadata_compiler PRIVATE irods_database_plugin_l3kvg l3kvg_engine l3kv_engine lite3-cpp conveyor gtest gtest_main irods_common irods_server dl libzmq-static)

add_executable(test_multihop_query tests/test_multihop_query.cpp)
target_link_libraries(test_multihop_query PRIVATE irods_database_plugin_l3kvg l3kvg_engine l3kv_engine lite3-cpp conveyor gtest gtest_main irods_common irods_server dl libzmq-static)

add_executable(test_advanced_operators tests/test_advanced_operators.cpp)
target_link_libraries(test_advanced_operators PRIVATE irods_database_plugin_l3kvg l3kvg_engine l3kv_engine lite3-cpp conveyor gtest gtest_main irods_common irods_server dl libzmq-static)

add_executable(test_nested_logic tests/test_nested_logic.cpp)
target_link_libraries(test_nested_logic PRIVATE irods_database_plugin_l3kvg l3kvg_engine l3kv_engine lite3-cpp conveyor gtest gtest_main irods_common irods_server dl libzmq-static)

add_executable(test_aggregates tests/test_aggregates.cpp)
target_link_libraries(test_aggregates PRIVATE irods_database_plugin_l3kvg l3kvg_engine l3kv_engine lite3-cpp conveyor gtest gtest_main irods_common irods_server dl libzmq-static)

add_executable(test_sorting tests/test_sorting.cpp)
target_link_libraries(test_sorting PRIVATE irods_database_plugin_l3kvg l3kvg_engine l3kv_engine lite3-cpp conveyor gtest gtest_main irods_common irods_server dl libzmq-static)

add_executable(test_groupby tests/test_groupby.cpp)
target_link_libraries(test_groupby PRIVATE irods_database_plugin_l3kvg l3kvg_engine l3kv_engine lite3-cpp conveyor gtest gtest_main irods_common irods_server dl libzmq-static)

add_executable(test_distinct tests/test_distinct.cpp)
target_link_libraries(test_distinct PRIVATE irods_database_plugin_l3kvg l3kvg_engine l3kv_engine lite3-cpp conveyor gtest gtest_main irods_common irods_server dl libzmq-static)

add_executable(test_deep_traversal tests/test_deep_traversal.cpp)
target_link_libraries(test_deep_traversal PRIVATE irods_database_plugin_l3kvg l3kvg_engine l3kv_engine lite3-cpp conveyor gtest gtest_main irods_common irods_server dl libzmq-static)

add_executable(test_legacy_compatibility tests/test_legacy_compatibility.cpp)
target_link_libraries(test_legacy_compatibility PRIVATE irods_database_plugin_l3kvg l3kvg_engine l3kv_engine lite3-cpp conveyor gtest gtest_main irods_common irods_server dl libzmq-static fmt)

# CPack Configuration
set(CPACK_PACKAGE_NAME "irods-database-plugin-l3kvg")
set(CPACK_PACKAGE_VENDOR "iRODS Consortium")
set(CPACK_PACKAGE_CONTACT "iRODS Consortium <support@irods.org>")
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "iRODS L3KVG Database Plugin")
set(CPACK_PACKAGE_VERSION "5.0.0")
set(CPACK_PACKAGE_RELEASE "0")
set(CPACK_DEBIAN_PACKAGE_MAINTAINER "iRODS Consortium <support@irods.org>")
set(CPACK_DEBIAN_PACKAGE_SECTION "contrib/science")
set(CPACK_DEBIAN_PACKAGE_PRIORITY "optional")
set(CPACK_DEBIAN_PACKAGE_ARCHITECTURE "amd64")
set(CPACK_DEBIAN_PACKAGE_DEPENDS "irods-server (>= 5.0.0), irods-runtime (>= 5.0.0), libzmq5")

# Install Logic
install(TARGETS irods_database_plugin_l3kvg
    LIBRARY DESTINATION /usr/lib/irods/plugins/database
)

include(CPack)
