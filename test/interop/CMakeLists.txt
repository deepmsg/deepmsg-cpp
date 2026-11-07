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

include(GoogleTest)
gtest_discover_tests(axc_interop_tests DISCOVERY_MODE PRE_TEST PROPERTIES LABELS interop)
