cmake_minimum_required(VERSION 3.20)
project(irods_database_plugin_l3kvg VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

add_compile_options(-Wno-changes-meaning)

set(IRODS_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../irods" CACHE PATH "Path to iRODS source tree")
set(IRODS_BUILD_DIR "${IRODS_DIR}/build" CACHE PATH "Path to iRODS build directory")

# Include Directories (active local development headers precede system headers)
include_directories(
    include
    ${CMAKE_CURRENT_SOURCE_DIR}/../l3kvg/include
    ${CMAKE_CURRENT_SOURCE_DIR}/../L3KV/src
    ${CMAKE_CURRENT_SOURCE_DIR}/../L3KV/src/engine
    ${CMAKE_CURRENT_SOURCE_DIR}/../lib/lite3-cpp/include
    ${CMAKE_CURRENT_SOURCE_DIR}/../yyjson/src
    ${IRODS_DIR}/lib/filesystem/include
    ${IRODS_DIR}/server/core/include
    ${IRODS_DIR}/lib/core/include
    ${IRODS_DIR}/lib/api/include
    ${IRODS_DIR}/server/genquery2/include
    ${IRODS_BUILD_DIR}/server/genquery2/dsl
)

# System iRODS headers fallback
if(EXISTS "/usr/include/irods")
    include_directories(/usr/include/irods)
endif()

# External Libraries
link_directories(
    /usr/lib/irods/plugins/database
    ${IRODS_BUILD_DIR}/lib
)

# Dependencies
find_package(PkgConfig REQUIRED)
pkg_check_modules(ZMQ REQUIRED libzmq)
if(ZMQ_FOUND AND NOT TARGET libzmq)
    add_library(libzmq INTERFACE)
    target_include_directories(libzmq INTERFACE ${ZMQ_INCLUDE_DIRS})
    target_link_libraries(libzmq INTERFACE ${ZMQ_LIBRARIES})
endif()
find_package(cppzmq REQUIRED)

# Subprojects
add_subdirectory(../l3kvg l3kvg)

# Core Plugin Library
add_library(irods_database_plugin_l3kvg SHARED
    src/db_plugin.cpp
    src/gq1_bridge.cpp
    src/catalog/catalog_facade.cpp
    src/compiler/gq2_compiler.cpp
    ${IRODS_DIR}/server/genquery2/src/genquery2_driver.cpp
    ${IRODS_BUILD_DIR}/server/genquery2/dsl/lexer.cpp
    ${IRODS_BUILD_DIR}/server/genquery2/dsl/parser.cpp
)

set_target_properties(irods_database_plugin_l3kvg PROPERTIES
    PREFIX "lib"
    OUTPUT_NAME "l3kvg"
)
target_compile_definitions(irods_database_plugin_l3kvg PRIVATE IRODS_SERVER)

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
        libzmq
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
add_plugin_test(test_plugin_environment tests/test_plugin_environment.cpp)

# Logic Tests (Direct Facade/Compiler)
add_executable(test_metadata_acls tests/test_metadata_acls.cpp)
target_link_libraries(test_metadata_acls PRIVATE irods_database_plugin_l3kvg l3kvg_engine l3kv_engine lite3-cpp conveyor gtest gtest_main irods_common irods_server dl libzmq)

add_executable(test_collections tests/test_collections.cpp)
target_link_libraries(test_collections PRIVATE irods_database_plugin_l3kvg l3kvg_engine l3kv_engine lite3-cpp conveyor gtest gtest_main irods_common irods_server dl libzmq)

add_executable(test_gq2_metadata_compiler tests/test_gq2_metadata_compiler.cpp)
target_link_libraries(test_gq2_metadata_compiler PRIVATE irods_database_plugin_l3kvg l3kvg_engine l3kv_engine lite3-cpp conveyor gtest gtest_main irods_common irods_server dl libzmq)

add_executable(test_multihop_query tests/test_multihop_query.cpp)
target_link_libraries(test_multihop_query PRIVATE irods_database_plugin_l3kvg l3kvg_engine l3kv_engine lite3-cpp conveyor gtest gtest_main irods_common irods_server dl libzmq)

add_executable(test_advanced_operators tests/test_advanced_operators.cpp)
target_link_libraries(test_advanced_operators PRIVATE irods_database_plugin_l3kvg l3kvg_engine l3kv_engine lite3-cpp conveyor gtest gtest_main irods_common irods_server dl libzmq)

add_executable(test_nested_logic tests/test_nested_logic.cpp)
target_link_libraries(test_nested_logic PRIVATE irods_database_plugin_l3kvg l3kvg_engine l3kv_engine lite3-cpp conveyor gtest gtest_main irods_common irods_server dl libzmq)

add_executable(test_aggregates tests/test_aggregates.cpp)
target_link_libraries(test_aggregates PRIVATE irods_database_plugin_l3kvg l3kvg_engine l3kv_engine lite3-cpp conveyor gtest gtest_main irods_common irods_server dl libzmq)

add_executable(test_sorting tests/test_sorting.cpp)
target_link_libraries(test_sorting PRIVATE irods_database_plugin_l3kvg l3kvg_engine l3kv_engine lite3-cpp conveyor gtest gtest_main irods_common irods_server dl libzmq)

add_executable(test_groupby tests/test_groupby.cpp)
target_link_libraries(test_groupby PRIVATE irods_database_plugin_l3kvg l3kvg_engine l3kv_engine lite3-cpp conveyor gtest gtest_main irods_common irods_server dl libzmq)

add_executable(test_distinct tests/test_distinct.cpp)
target_link_libraries(test_distinct PRIVATE irods_database_plugin_l3kvg l3kvg_engine l3kv_engine lite3-cpp conveyor gtest gtest_main irods_common irods_server dl libzmq)

add_executable(test_deep_traversal tests/test_deep_traversal.cpp)
target_link_libraries(test_deep_traversal PRIVATE irods_database_plugin_l3kvg l3kvg_engine l3kv_engine lite3-cpp conveyor gtest gtest_main irods_common irods_server dl libzmq)

add_executable(test_legacy_compatibility tests/test_legacy_compatibility.cpp)
target_link_libraries(test_legacy_compatibility PRIVATE irods_database_plugin_l3kvg l3kvg_engine l3kv_engine lite3-cpp conveyor gtest gtest_main irods_common irods_server dl libzmq)

add_executable(test_gq1_bridge tests/test_gq1_bridge.cpp src/gq1_bridge.cpp src/compiler/gq2_compiler.cpp src/catalog/catalog_facade.cpp)
target_link_libraries(test_gq1_bridge PRIVATE l3kvg_engine l3kv_engine lite3-cpp conveyor gtest gtest_main irods_common irods_server dl libzmq fmt)

add_executable(bootstrap_l3kvg tests/bootstrap_l3kvg.cpp src/catalog/catalog_facade.cpp src/compiler/gq2_compiler.cpp)
target_link_libraries(bootstrap_l3kvg PRIVATE l3kvg_engine l3kv_engine lite3-cpp conveyor irods_common irods_server dl libzmq fmt)

add_executable(test_repro_imkdir tests/test_repro_imkdir.cpp src/gq1_bridge.cpp src/compiler/gq2_compiler.cpp src/catalog/catalog_facade.cpp)
target_link_libraries(test_repro_imkdir PRIVATE l3kvg_engine l3kv_engine lite3-cpp conveyor gtest gtest_main irods_common irods_server dl libzmq fmt)

add_executable(test_repro_icp tests/test_repro_icp.cpp src/gq1_bridge.cpp src/compiler/gq2_compiler.cpp src/catalog/catalog_facade.cpp)
target_link_libraries(test_repro_icp PRIVATE l3kvg_engine l3kv_engine lite3-cpp conveyor gtest gtest_main irods_common irods_server dl libzmq fmt)

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
    LIBRARY DESTINATION lib/irods/plugins/database
)

install(TARGETS l3kvg_server bootstrap_l3kvg
    RUNTIME DESTINATION bin
)

set(CPACK_SET_DESTDIR ON)
set(CPACK_DEBIAN_PACKAGE_MAINTAINER "Gemini")
include(CPack)
