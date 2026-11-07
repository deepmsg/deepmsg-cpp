axc_add_executable(axc_unit_tests
    SOURCES
        core/version_test.cpp
        core/fixed_string_test.cpp
        core/long2long_hash_map_test.cpp
        core/clock_test.cpp
        core/idle_strategy_test.cpp
        core/cpu_affinity_test.cpp
        core/agent_runner_test.cpp
        core/deepmsg_client_test.cpp
        core/io_test.cpp
        core/config_test.cpp
        core/trace_test.cpp
    DEPS
        axc_core
        GTest::gtest_main
        aeron_driver_static
        aeron_static)

target_compile_definitions(axc_unit_tests PRIVATE "AXC_GOLDEN_DIR=\"${PROJECT_SOURCE_DIR}/test/golden\"")

# Separate binary so the enabled StageTimer path is compiled and measured even
# though AXC_STAGE_TIMING is off for normal builds.
axc_add_executable(axc_stage_timer_timing_test
    SOURCES
        core/stage_timer_timing_test.cpp
    DEPS
        axc_core
        GTest::gtest_main)

target_compile_definitions(axc_stage_timer_timing_test PRIVATE AXC_STAGE_TIMING=1)

axc_add_executable(axc_archive_unit_tests
    SOURCES
        archive/server/catalog_index_test.cpp
        archive/server/catalog_test.cpp
        archive/server/catalog_golden_test.cpp
    DEPS
        axc_archive_server
        GTest::gtest_main)

target_compile_definitions(axc_archive_unit_tests PRIVATE
    "AXC_GOLDEN_DIR=\"${PROJECT_SOURCE_DIR}/test/golden\"")

include(GoogleTest)
# PRE_TEST: discover at test time, not build time. Sanitizer binaries (TSan in
# particular) cannot always be executed by the build step -- on kernels with high
# ASLR entropy TSan aborts with "unexpected memory mapping" unless run under
# `setarch -R`.
gtest_discover_tests(axc_unit_tests DISCOVERY_MODE PRE_TEST PROPERTIES LABELS unit)
gtest_discover_tests(axc_stage_timer_timing_test DISCOVERY_MODE PRE_TEST PROPERTIES LABELS unit)
gtest_discover_tests(axc_archive_unit_tests DISCOVERY_MODE PRE_TEST PROPERTIES LABELS unit)
