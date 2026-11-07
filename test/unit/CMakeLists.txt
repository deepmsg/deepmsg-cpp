axc_add_executable(axc_unit_tests
    SOURCES
        core/version_test.cpp
    DEPS
        axc_core
        GTest::gtest_main)

include(GoogleTest)
# PRE_TEST: discover at test time, not build time. Sanitizer binaries (TSan in
# particular) cannot always be executed by the build step -- on kernels with high
# ASLR entropy TSan aborts with "unexpected memory mapping" unless run under
# `setarch -R`.
gtest_discover_tests(axc_unit_tests DISCOVERY_MODE PRE_TEST)
