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
        fixture/c_driver.cpp
        fixture/java_process.cpp
        fixture/process.cpp
    DEPS
        axc_archive_server
        axc_core
        GTest::gtest_main)

target_include_directories(axc_archive_interop_tests PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}")

# The reference `aeron-all` jar is a build product of the pinned submodule
# (`./gradlew :aeron-all:assemble`), not a checked-in artifact; a missing jar
# skips the binary loudly instead of pretending to pass.
set(AXC_AERON_ALL_JAR
    "${PROJECT_SOURCE_DIR}/third_party/aeron/aeron-all/build/libs/aeron-all-1.54.0.jar")

target_compile_definitions(axc_archive_interop_tests PRIVATE
    "AXC_AERON_JAR=\"${AXC_AERON_ALL_JAR}\""
    "AXC_AERONMD_DEFAULT=\"${PROJECT_BINARY_DIR}/aeron/binaries/aeronmd\"")

include(GoogleTest)
gtest_discover_tests(axc_interop_tests DISCOVERY_MODE PRE_TEST PROPERTIES LABELS interop)
gtest_discover_tests(axc_archive_interop_tests DISCOVERY_MODE PRE_TEST PROPERTIES LABELS interop)
