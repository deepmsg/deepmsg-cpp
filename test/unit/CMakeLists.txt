axc_add_executable(axc_unit_tests
    SOURCES
        core/version_test.cpp
        core/fixed_string_test.cpp
        core/long2long_hash_map_test.cpp
        core/expandable_ring_buffer_test.cpp
        core/clock_test.cpp
        core/idle_strategy_test.cpp
        core/cpu_affinity_test.cpp
        core/agent_runner_test.cpp
        core/deepmsg_client_test.cpp
        core/io_test.cpp
        core/config_test.cpp
        core/trace_test.cpp
        core/many_to_one_queue_test.cpp
        core/duty_cycle_tracker_test.cpp
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
        archive/io/segment_reader_test.cpp
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
        archive/server/replication_validation_test.cpp
        archive/server/replication_session_test.cpp
        archive/server/session_worker_test.cpp
        archive/server/threading_test.cpp
        archive/server/config_registry_test.cpp
    DEPS
        axc_archive_server
        aeron_static
        GTest::gtest_main)

target_compile_definitions(axc_archive_unit_tests PRIVATE
    "AXC_GOLDEN_DIR=\"${PROJECT_SOURCE_DIR}/test/golden\"")

axc_add_executable(axc_cluster_unit_tests
    SOURCES
        cluster/common/recording_log_test.cpp
        cluster/common/recording_log_golden_test.cpp
        cluster/common/node_state_file_test.cpp
        cluster/common/cluster_member_test.cpp
        cluster/common/cluster_counters_test.cpp
        cluster/common/cluster_config_test.cpp
        cluster/common/cluster_mark_file_test.cpp
        cluster/common/cluster_mark_file_golden_test.cpp
        cluster/common/priority_heap_timer_service_test.cpp
        cluster/common/wheel_timer_service_test.cpp
        cluster/common/timer_golden_test.cpp
        cluster/service/container_components_test.cpp
        cluster/service/basic_auction_test.cpp
    DEPS
        axc_cluster_common
        axc_cluster_service
        axc_basic_auction_service
        GTest::gtest_main)

target_compile_definitions(axc_cluster_unit_tests PRIVATE
    "AXC_GOLDEN_DIR=\"${PROJECT_SOURCE_DIR}/test/golden\"")

# C3a: the client side needs a media driver (the scripted cluster runs on an
# embedded one), so it is its own binary.
axc_add_executable(axc_cluster_client_unit_tests
    SOURCES
        cluster/client/client_config_test.cpp
        cluster/client/egress_poller_test.cpp
        cluster/client/async_connect_test.cpp
        cluster/client/cluster_client_test.cpp
        cluster/client/egress_adapter_test.cpp
        cluster/client/session_event_codec_compatibility_test.cpp
    DEPS
        axc_cluster_client
        aeron_driver_static
        aeron_static
        GTest::gtest_main)

# C5a: the election seam (gmock host + sender) and the states it transcribes.
axc_add_executable(axc_cluster_election_unit_tests
    SOURCES
        cluster/election/election_test.cpp
    DEPS
        axc_cluster_election
        GTest::gmock
        GTest::gtest_main)

# C4a: the CM context/conclude rules and the Q6 standby startup check.
axc_add_executable(axc_cluster_consensus_unit_tests
    SOURCES
        cluster/consensus/consensus_module_context_test.cpp
        cluster/consensus/standby_startup_check_test.cpp
        cluster/consensus/session_manager_test.cpp
        cluster/consensus/log_publisher_test.cpp
        cluster/consensus/ingress_adapter_test.cpp
        cluster/consensus/service_ack_test.cpp
        cluster/consensus/service_proxy_test.cpp
        cluster/consensus/pending_service_message_tracker_test.cpp
        cluster/consensus/consensus_module_adapter_test.cpp
    DEPS
        axc_cluster_consensus
        axc_core
        aeron_driver_static
        aeron_static
        GTest::gtest_main)

# C3c: the same schema transformed to version 5, for the compatibility test.
if (AXC_WITH_SBE)
    add_dependencies(axc_cluster_client_unit_tests axc_sbe_cluster_codecs_v5)
    target_include_directories(axc_cluster_client_unit_tests PRIVATE
        "${PROJECT_BINARY_DIR}/generated/axc/sbe-v5")
endif ()

include(GoogleTest)
# PRE_TEST: discover at test time, not build time. Sanitizer binaries (TSan in
# particular) cannot always be executed by the build step -- on kernels with high
# ASLR entropy TSan aborts with "unexpected memory mapping" unless run under
# `setarch -R`.
gtest_discover_tests(axc_unit_tests DISCOVERY_MODE PRE_TEST PROPERTIES LABELS unit TIMEOUT 120)
gtest_discover_tests(axc_stage_timer_timing_test DISCOVERY_MODE PRE_TEST PROPERTIES LABELS unit TIMEOUT 120)
gtest_discover_tests(axc_archive_unit_tests DISCOVERY_MODE PRE_TEST PROPERTIES LABELS unit TIMEOUT 120)
gtest_discover_tests(axc_cluster_unit_tests DISCOVERY_MODE PRE_TEST PROPERTIES LABELS unit TIMEOUT 120)
gtest_discover_tests(axc_cluster_client_unit_tests DISCOVERY_MODE PRE_TEST PROPERTIES LABELS unit TIMEOUT 120)
gtest_discover_tests(axc_cluster_election_unit_tests DISCOVERY_MODE PRE_TEST PROPERTIES LABELS unit TIMEOUT 120)
gtest_discover_tests(axc_cluster_consensus_unit_tests DISCOVERY_MODE PRE_TEST PROPERTIES LABELS unit TIMEOUT 120)
