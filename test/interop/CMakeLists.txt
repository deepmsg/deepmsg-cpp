axc_add_executable(axc_interop_tests
    SOURCES
        fixture/c_driver.cpp
        fixture/java_process.cpp
        fixture/process.cpp
        fixture/fixture_test.cpp
    DEPS
        axc_core
        GTest::gtest_main)

target_include_directories(axc_interop_tests PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}")

target_compile_definitions(axc_interop_tests PRIVATE
    "AXC_INTEROP_DIR=\"${CMAKE_CURRENT_SOURCE_DIR}\""
    "AXC_AERONMD_DEFAULT=\"${PROJECT_BINARY_DIR}/aeron/binaries/aeronmd\"")

# A1a: the reference `ArchiveTool` as the oracle for a catalog we wrote.
axc_add_executable(axc_archive_interop_tests
    SOURCES
        archive/catalog_interop_test.cpp
        archive/conductor_interop_test.cpp
        archive/recording_interop_test.cpp
        archive/replication_client_probe_test.cpp
        archive/replication_interop_test.cpp
        archive/threading_interop_test.cpp
        archive/reference_java_client_test.cpp
        archive/content_parity_test.cpp
        archive/archive_tool_ro_interop_test.cpp
        fixture/c_driver.cpp
        fixture/java_process.cpp
        fixture/process.cpp
    DEPS
        axc_archive_server
        axc_core
        GTest::gtest_main)

target_include_directories(axc_archive_interop_tests PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}")

target_compile_definitions(axc_archive_interop_tests PRIVATE
    "AXC_ARCHIVE_SERVER_BIN=\"${PROJECT_BINARY_DIR}/apps/deepmsg-archive-server/deepmsg-archive-server\"")

# The reference `aeron-all` jar is a build product of the pinned submodule
# (`./gradlew :aeron-all:assemble`), not a checked-in artifact; a missing jar
# skips the binary loudly instead of pretending to pass.
set(AXC_AERON_ALL_JAR
    "${PROJECT_SOURCE_DIR}/third_party/aeron/aeron-all/build/libs/aeron-all-1.54.0.jar")

target_compile_definitions(axc_archive_interop_tests PRIVATE
    "AXC_AERON_JAR=\"${AXC_AERON_ALL_JAR}\""
    "AXC_ARCHIVE_TOOL_RO_BIN=\"${PROJECT_BINARY_DIR}/tools/archive-tool-ro/archive-tool-ro\""
    "AXC_AERONMD_DEFAULT=\"${PROJECT_BINARY_DIR}/aeron/binaries/aeronmd\"")

# C1a: the reference `ClusterTool recording-log` as the oracle for a
# `recording.log` we wrote.
axc_add_executable(axc_cluster_interop_tests
    SOURCES
        cluster/recording_log_interop_test.cpp
        cluster/cluster_mark_file_interop_test.cpp
        cluster/container_lc4a_interop_test.cpp
        cluster/container_snapshot_interop_test.cpp
        cluster/container_binary_interop_test.cpp
        cluster/client_connect_interop_test.cpp
        fixture/java_process.cpp
        fixture/process.cpp
    DEPS
        axc_cluster_common
        axc_cluster_service
        axc_cluster_client
        axc_basic_auction_service
        axc_core
        aeron_static
        aeron_archive_c_client_static
        GTest::gtest_main)

target_include_directories(axc_cluster_interop_tests PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}")

# C3b: the C ABI smoke program is C (not C++), linked against the C++ library.
axc_add_executable(deepmsg-cluster-client-smoke
    SOURCES
        cluster/c_client_smoke.c
    DEPS
        axc_cluster_client)
set_target_properties(deepmsg-cluster-client-smoke PROPERTIES LINKER_LANGUAGE CXX)

target_compile_definitions(axc_cluster_interop_tests PRIVATE
    "AXC_AERON_JAR=\"${AXC_AERON_ALL_JAR}\""
    "AXC_GOLDEN_DIR=\"${PROJECT_SOURCE_DIR}/test/golden\""
    "AXC_CONTAINER_BINARY=\"$<TARGET_FILE:deepmsg-clustered-service-container>\""
    "AXC_ECHO_SHARED_LIBRARY=\"$<TARGET_FILE:echo_service_dynamic>\""
    "AXC_CLUSTER_CLIENT_SMOKE=\"$<TARGET_FILE:deepmsg-cluster-client-smoke>\"")

add_dependencies(axc_cluster_interop_tests deepmsg-clustered-service-container echo_service_dynamic
    deepmsg-cluster-client-smoke)

# The tool under test is an executable of its own; build it with the suite.
add_dependencies(axc_archive_interop_tests archive-tool-ro)

include(GoogleTest)
gtest_discover_tests(axc_interop_tests DISCOVERY_MODE PRE_TEST PROPERTIES LABELS interop)
gtest_discover_tests(axc_archive_interop_tests DISCOVERY_MODE PRE_TEST PROPERTIES LABELS interop)
gtest_discover_tests(axc_cluster_interop_tests DISCOVERY_MODE PRE_TEST PROPERTIES LABELS interop)
