# C4f: R1 for the cluster -- a separate binary that replaces the global
# allocator and counts per labelled agent thread. Not built under sanitizers:
# ASan owns the allocator there.
if (NOT AXC_SANITIZER)
    axc_add_executable(axc_cluster_zero_alloc_test
        SOURCES
            alloc_probe.cpp
            cluster_zero_alloc_test.cpp
        DEPS
            axc_cluster_consensus
            axc_cluster_service
            axc_cluster_client
            axc_cluster_common
            axc_archive_server
            axc_echo_service
            axc_core
            aeron_driver_static
            aeron_static
            aeron_archive_c_client_static
            GTest::gtest_main)

    target_include_directories(axc_cluster_zero_alloc_test PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}")

    include(GoogleTest)
    # 10^4 warm-up + 10^5 measured round trips; generous enough for a debug build.
    gtest_discover_tests(axc_cluster_zero_alloc_test DISCOVERY_MODE PRE_TEST PROPERTIES LABELS alloc TIMEOUT 900)
endif ()
