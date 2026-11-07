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
    DEPS
        axc_core
        GTest::gtest_main
        aeron_driver_static
        aeron_static)

target_compile_definitions(axc_unit_tests PRIVATE "AXC_GOLDEN_DIR=\"${PROJECT_SOURCE_DIR}/test/golden\"")

include(GoogleTest)
# PRE_TEST: discover at test time, not build time. Sanitizer binaries (TSan in
# particular) cannot always be executed by the build step -- on kernels with high
# ASLR entropy TSan aborts with "unexpected memory mapping" unless run under
# `setarch -R`.
gtest_discover_tests(axc_unit_tests DISCOVERY_MODE PRE_TEST)
