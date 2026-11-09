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
        archive/server/checksum_test.cpp
        archive/server/recording_writer_test.cpp
        archive/server/recording_reader_test.cpp
        archive/server/archive_mark_file_test.cpp
        archive/server/archive_mark_file_golden_test.cpp
        archive/server/authenticator_test.cpp
        archive/server/control_session_test.cpp
        archive/server/control_session_adapter_test.cpp
        archive/io/sync_writer_test.cpp
        archive/server/catalog_test.cpp
        archive/server/catalog_golden_test.cpp
        archive/server/channel_uri_test.cpp
        archive/server/archive_config_test.cpp
        archive/server/archive_counters_test.cpp
        archive/server/archive_launch_test.cpp
        archive/server/recording_pos_test.cpp
        archive/server/recording_session_test.cpp
        archive/server/recording_events_test.cpp
        archive/server/replay_session_test.cpp
        archive/server/create_replay_publication_session_test.cpp
        archive/server/segment_files_test.cpp
        archive/server/delete_segments_session_test.cpp
        archive/server/segment_validation_test.cpp
        archive/server/list_recordings_session_test.cpp
    DEPS
        axc_archive_server
        aeron_static
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
